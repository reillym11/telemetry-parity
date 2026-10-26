import datetime as dt
import decimal
from pathlib import Path

import boto3
import pyarrow.dataset as ds

from harness.config import BUCKET


class Lake:
    def __init__(self, endpoint):
        self.s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="minioadmin",
                               aws_secret_access_key="minioadmin", region_name="us-east-1")

    def keys(self, prefix="") -> list:
        out, token = [], None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix, **({"ContinuationToken": token} if token else {})}
            r = self.s3.list_objects_v2(**kw)
            out += [o["Key"] for o in r.get("Contents", [])]
            if not r.get("IsTruncated"):
                return out
            token = r["NextContinuationToken"]

    def buckets(self) -> list:
        return [b["Name"] for b in self.s3.list_buckets().get("Buckets", [])]

    def put(self, key, path):
        self.s3.upload_file(str(path), BUCKET, key)

    def download(self, prefix, dest: Path) -> list:
        files = []
        for k in self.keys(prefix):
            if not k.endswith(".parquet"):
                continue
            target = dest / k[len(prefix):].lstrip("/")
            target.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file(BUCKET, k, str(target))
            files.append(target)
        return files


def fmt_ts(v: dt.datetime) -> str:
    if v.tzinfo is None:
        v = v.replace(tzinfo=dt.timezone.utc)
    v = v.astimezone(dt.timezone.utc)
    return v.strftime("%Y-%m-%dT%H:%M:%S.") + f"{v.microsecond // 1000:03d}Z"


def _norm(v):
    if isinstance(v, dt.datetime):
        return fmt_ts(v)
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, float):
        return decimal.Decimal(repr(v))
    return v


def read_parquet(root: Path):
    """Rows as dicts (decimals as Decimal, timestamps as RFC 3339 ms Z) plus the arrow schema as strings."""
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return [], {}
    dataset = ds.dataset([str(f) for f in files], format="parquet", partitioning="hive",
                         partition_base_dir=str(root))
    table = dataset.to_table()
    types = {f.name: str(f.type) for f in table.schema}
    rows = [{k: _norm(v) for k, v in r.items()} for r in table.to_pylist()]
    return rows, types
