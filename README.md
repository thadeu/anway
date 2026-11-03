<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="brand/png/lockup-on-dark.png">
    <img src="brand/png/lockup-on-light.png" alt="Anway" width="320">
  </picture>
</p>

# Anway

A [Clowk](https://clowk.in) product, planned at `anway.clowk.in`. A language-agnostic background job server. It is one binary with embedded disk storage, and it does not need Redis.

Any language can push a job, and a worker in another language can run it. For example, a Rails app pushes `email.send`, and a Go worker runs it.

> Status: phase 1 core.

## Run

```bash
mise install
go build -o bin/server ./cmd/server
DATA_DIR=./data ./bin/server
```

## Docker

```bash
docker run -p 7419:7419 -e SERVER_PASSWORD=change-me -v anway-data:/data ghcr.io/thadeu/anway:latest
```

See [docs/dev/docker.md](docs/dev/docker.md).

## Download

Each `v*` tag publishes binaries for Linux and macOS, on x86-64 and arm64, at [Releases](https://github.com/thadeu/anway/releases).

Configure the server with env vars or flags. See [docs/dev/configuration.md](docs/dev/configuration.md).

## Features

- Queues with priority order, pause, and resume.
- Scheduled jobs, retries with backoff, and a dead set.
- `MUTATE` for retries, scheduled jobs, and the dead set.
- Cron, unique jobs, expiring jobs, and queue throttling.
- Batches with callbacks and child batches, and job tracking.
- A death hook that pushes a notice job when a job dies.
- An HTTP API for the web UI.

There is no web UI yet.

## Goals

- **Agnostic:** Producers and workers use a simple TCP protocol.
- **Low memory:** Job data stays on disk, and memory use has a configured limit.
- **Reliable:** A confirmed job survives a process crash.
- **Simple operation:** One binary and one data directory.
- **Client compatibility:** Version 1 follows the public Faktory work protocol, so existing clients can connect.

## Documentation

- [SPEC.md](SPEC.md): Product requirements and scope.
- [docs/dev/architecture.md](docs/dev/architecture.md): Components and data flow.
- [docs/dev/protocol.md](docs/dev/protocol.md): Wire protocol.
- [docs/dev/job-lifecycle.md](docs/dev/job-lifecycle.md): Job states and retries.
- [docs/dev/storage.md](docs/dev/storage.md): Storage design.
- [docs/dev/batches.md](docs/dev/batches.md): Batches and job tracking.
- [docs/dev/http-api.md](docs/dev/http-api.md): HTTP API for the web UI.
- [docs/dev/configuration.md](docs/dev/configuration.md): Configuration.
- [docs/dev/docker.md](docs/dev/docker.md): Docker image.
- [docs/dev/tls.md](docs/dev/tls.md): TLS with Let's Encrypt or Caddy.
- [docs/dev/clients.md](docs/dev/clients.md): Client repositories and rules.
- [docs/dev/development.md](docs/dev/development.md): Development rules.

## License

The server is licensed under [AGPL-3.0](LICENSE). The clients use MIT.
