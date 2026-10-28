<p align="center">
  <picture>
    <source
      media="(prefers-color-scheme: dark)"
      srcset="https://raw.githubusercontent.com/Lazypilottt/s3f/main/public/banner-dark.svg">
    <source
      media="(prefers-color-scheme: light)"
      srcset="https://raw.githubusercontent.com/Lazypilottt/s3f/main/public/banner-cream.svg">
    <img
      alt="s3f — S3 ops, but fearlessly fast"
      src="https://raw.githubusercontent.com/Lazypilottt/s3f/main/public/banner-cream.svg"
      width="800">
  </picture>
</p>

<p align="center">
  <strong>S3 ops, but fearlessly fast.</strong><br>
  <sub>Built in Rust. Streaming. Parallel. No bloat.</sub>
</p>

---

Python bindings for [s3f](https://github.com/Lazypilottt/s3f) — a lightweight,
high-throughput S3 library built in Rust.

## Install

```bash
uv add s3f        # recommended
pip install s3f
pipx install s3f  # isolated install
```

## Quick start

```python
from s3f import Config, S3Client, CredentialSource

config = Config("us-east-1", CredentialSource.Env)
client = S3Client(config)

# Upload
results = client.upload(["./data"], "my-bucket", "uploads/")
for r in results:
    print(f"{r.source} -> s3://{r.key} ({r.parts} parts)")

# Download
files = client.download("my-bucket", "uploads/", "./out")

# List
objects = client.list("my-bucket", "uploads/")
```

### S3-compatible backends

```python
from s3f import Config, S3Client, static_credentials

config = Config(
    "us-east-1",
    static_credentials("access-key", "secret-key"),
    endpoint="http://localhost:9000",  # MinIO, SeaweedFS, Garage, etc.
)
client = S3Client(config)
```

## License

[MIT](https://github.com/Lazypilottt/s3f/blob/main/LICENSE)
