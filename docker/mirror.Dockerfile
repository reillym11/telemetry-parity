# Self-test stand-in for a Rust job. Implements docs/RUST_JOB_CONTRACT.md by replaying legacy output.
FROM python:3.11
RUN pip install --no-cache-dir confluent-kafka==2.16.0 pyarrow==26.0.0 boto3==1.43.118
WORKDIR /app
COPY harness/__init__.py harness/murmur2.py /app/harness/
COPY harness/mirror/ /app/harness/mirror/
ENTRYPOINT ["python", "-m", "harness.mirror"]
