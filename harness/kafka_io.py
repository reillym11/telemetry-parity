import json
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from harness.murmur2 import java_partition


@dataclass
class Message:
    partition: int
    offset: int
    key: str | None
    raw: str

    def as_json(self):
        return {"partition": self.partition, "offset": self.offset, "key": self.key, "value": self.raw}


class Kafka:
    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self.admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})

    def wait_ready(self, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                self.admin.list_topics(timeout=2)
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError(f"Kafka at {self.bootstrap} not ready after {timeout}s")

    def list_topics(self) -> set:
        return set(self.admin.list_topics(timeout=10).topics)

    def create_topic(self, name, partitions, cleanup_policy):
        fs = self.admin.create_topics([NewTopic(name, partitions, 1, config={"cleanup.policy": cleanup_policy})])
        for f in fs.values():
            f.result(timeout=30)

    def partitions(self, topic) -> list:
        return sorted(self.admin.list_topics(topic, timeout=10).topics[topic].partitions)

    def end_offsets(self, topic) -> dict:
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "parity-probe", "enable.auto.commit": False,
                      "log_level": 3})
        try:
            return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in self.partitions(topic)}
        finally:
            c.close()

    def committed(self, group, topic, consumer=None) -> dict:
        parts = [TopicPartition(topic, p) for p in self.partitions(topic)]
        c = consumer or self.offset_reader(group)
        try:
            deadline = time.monotonic() + 60
            while True:
                try:
                    return {tp.partition: max(tp.offset, 0) for tp in c.committed(parts, timeout=10)}
                except KafkaException:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(1)
        finally:
            if consumer is None:
                c.close()

    def offset_reader(self, group, isolation="read_committed") -> Consumer:
        # read_committed waits for any open transaction's pending offsets (require_stable)
        return Consumer({"bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": False,
                         "isolation.level": isolation, "log_level": 3})

    def load_fixture(self, path, topic, partitions) -> dict:
        """Produces each line keyed by vin in file order with the Java-compatible murmur2 partitioner."""
        misplaced, count, errors = [], 0, []

        def report(err, msg):
            if err is not None:
                errors.append(str(err))
            elif msg.partition() != java_partition(msg.key(), partitions):
                misplaced.append((msg.key().decode(), msg.partition()))

        p = Producer({"bootstrap.servers": self.bootstrap, "partitioner": "murmur2_random",
                      "enable.idempotence": True, "linger.ms": 5, "log_level": 3})
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                vin = json.loads(line)["vin"]
                p.produce(topic, key=vin.encode(), value=line.encode(), on_delivery=report)
                count += 1
                p.poll(0)
        p.flush(60)
        if errors:
            raise RuntimeError(f"fixture load failed: {errors[:3]}")
        return {"records": count, "misplaced": misplaced}

    def capture(self, topic, isolation="read_committed") -> list:
        parts = self.partitions(topic)
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"parity-capture-{time.monotonic_ns()}",
                      "enable.auto.commit": False, "isolation.level": isolation, "enable.partition.eof": True, "log_level": 3})
        c.assign([TopicPartition(topic, p, 0) for p in parts])
        done, out = set(), []
        try:
            while len(done) < len(parts):
                msg = c.poll(5.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        done.add(msg.partition())
                        continue
                    raise RuntimeError(str(msg.error()))
                done.discard(msg.partition())
                out.append(Message(msg.partition(), msg.offset(),
                                   msg.key().decode() if msg.key() is not None else None, msg.value().decode()))
        finally:
            c.close()
        return sorted(out, key=lambda m: (m.partition, m.offset))
