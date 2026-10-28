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

Node.js bindings for [s3f](https://github.com/Lazypilottt/s3f) — a lightweight,
high-throughput S3 library built in Rust.

## Install

```bash
npm install @lazypilottt/s3f   # npm
pnpm add @lazypilottt/s3f      # pnpm
yarn add @lazypilottt/s3f      # yarn
bun add @lazypilottt/s3f       # bun
```

## Quick start

```typescript
import { S3Client } from "@lazypilottt/s3f";

const client = await S3Client.create({ region: "us-east-1" });

// Upload
const uploaded = await client.upload({
  sources: ["./data"],
  bucket: "my-bucket",
  prefix: "uploads/",
});

// Download
const downloaded = await client.download({
  bucket: "my-bucket",
  prefix: "uploads/",
  destDir: "./out",
});

// List
const objects = await client.list({
  bucket: "my-bucket",
  prefix: "uploads/",
});
```

### S3-compatible backends

```typescript
const client = await S3Client.create({
  region: "us-east-1",
  accessKey: "access-key",
  secretKey: "secret-key",
  endpoint: "http://localhost:9000", // MinIO, SeaweedFS, Garage, etc.
});
```

## License

[MIT](https://github.com/Lazypilottt/s3f/blob/main/LICENSE)
