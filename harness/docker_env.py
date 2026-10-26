import os
import subprocess
from pathlib import Path

from harness.config import ROOT


class HarnessError(RuntimeError):
    pass


class Docker:
    def __init__(self, external_host: str):
        self.env = dict(os.environ, KAFKA_EXTERNAL_HOST=external_host)

    def _run(self, args, log: Path | None = None, check=True, capture=False):
        kw = {"cwd": ROOT, "env": self.env, "text": True}
        if log is not None:
            with open(log, "a") as f:
                r = subprocess.run(args, stdout=f, stderr=subprocess.STDOUT, **kw)
        elif capture:
            r = subprocess.run(args, capture_output=True, **kw)
        else:
            r = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, **kw)
        if check and r.returncode != 0:
            detail = (r.stderr or "") if log is None else f"see {log}"
            raise HarnessError(f"{' '.join(args[:4])} ... failed ({r.returncode}): {detail.strip()[-800:]}")
        return r

    def compose(self, *args, **kw):
        return self._run(["docker", "compose", *args], **kw)

    def docker(self, *args, **kw):
        return self._run(["docker", *args], **kw)

    def clean_slate(self, log: Path):
        self.compose("--profile", "jobs", "down", "-v", "--remove-orphans", log=log)
        self.compose("up", "-d", "--wait", "kafka", "minio", log=log)

    def has_image(self, image) -> bool:
        return self.docker("image", "inspect", image, check=False).returncode == 0

    def ensure_spark_runner(self, spark_sha: str, log: Path) -> str:
        image = os.environ.get("SPARK_RUNNER_IMAGE") or f"telemetry-parity/spark-runner:{spark_sha[:12]}"
        self.env["SPARK_RUNNER_IMAGE"] = image
        if self.has_image(image):
            return image
        if os.environ.get("SPARK_RUNNER_IMAGE"):
            self.docker("pull", image, log=log)
        else:
            self.compose("--profile", "jobs", "build", "spark-runner", log=log)
        return image

    def spark_submit(self, job, output: str, log: Path, s3: bool):
        args = ["--profile", "jobs", "run", "--rm", "-T", "spark-runner", "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--conf", "spark.sql.session.timeZone=UTC",
                "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=UTC",
                "--conf", "spark.jars.ivy=/tmp/.ivy2",
                "--class", job.spark_class, f"/opt/jobs/{job.name}.jar",
                "--bootstrap", "kafka:9092", "--checkpoint", f"/tmp/checkpoints/{job.name}", "--output", output]
        if s3:
            args += ["--s3-endpoint", "http://minio:9000"]
        self.compose(*args, log=log)

    def start_rust(self, name: str, image: str, env: dict, log: Path):
        self.env["RUST_IMAGE"] = image
        args = ["--profile", "jobs", "run", "-d", "--name", name]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        self.compose(*args, "rust-job", log=log)

    def kill(self, name):
        self.docker("kill", "-s", "KILL", name)

    def restart(self, name):
        self.docker("start", name)

    def wait(self, name) -> int:
        return int(self.docker("wait", name, capture=True).stdout.strip().splitlines()[-1])

    def logs(self, name, path: Path):
        with open(path, "a") as f:
            subprocess.run(["docker", "logs", name], stdout=f, stderr=subprocess.STDOUT, cwd=ROOT)

    def remove(self, name):
        self.docker("rm", "-f", name, check=False)

    def build(self, dockerfile: str, tags: list, log: Path):
        args = ["build", "-f", dockerfile]
        for t in tags:
            args += ["-t", t]
        if self.docker(*args, ".", log=log, check=False).returncode != 0:
            # BuildKit re-resolves the base image on every build, which hits Docker Hub rate limits;
            # the classic builder uses the local copy.
            self.env["DOCKER_BUILDKIT"] = "0"
            try:
                self.docker(*args, ".", log=log)
            finally:
                self.env.pop("DOCKER_BUILDKIT")
