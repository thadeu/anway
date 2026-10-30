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

## Features

- Queues with priority order, pause, and resume.
- Scheduled jobs, retries with backoff, and a dead set.
- `MUTATE` for retries, scheduled jobs, and the dead set.
- Cron, unique jobs, expiring jobs, and queue throttling.
- Batches with callbacks and child batches, and job tracking.

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
- [docs/dev/configuration.md](docs/dev/configuration.md): Configuration.
- [docs/dev/tls.md](docs/dev/tls.md): TLS with Let's Encrypt or Caddy.
- [docs/dev/clients.md](docs/dev/clients.md): Client repositories and rules.
- [docs/dev/development.md](docs/dev/development.md): Development rules.

## License

The server is licensed under [AGPL-3.0](LICENSE). The clients use MIT.
