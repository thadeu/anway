# Specification

Requirement levels: **MUST** is mandatory for the phase. **SHOULD** is expected, unless there is a documented reason.

## 1. Problem

Apps in Go, Rails, and Node/Bun need to share background work. Redis-based tools keep data in memory, so a large backlog can use all available memory. Faktory is agnostic, but it also depends on Redis.

## 2. Product

A job server that:

- Accepts jobs from any language.
- Keeps jobs on disk.
- Delivers each job at least once.
- Gives operators a web UI.

The long-term product is a paid monthly SaaS. The core comes first.

## 3. Phase 1 scope: core

### Functional requirements

- FR1: The server MUST implement the handshake and these commands: `PUSH`, `FETCH`, `ACK`, `FAIL`, `BEAT`, `INFO`, `FLUSH`, `END`.
- FR2: The server MUST support many named queues. `FETCH` checks queues in the given order.
- FR3: The server MUST support scheduled jobs through the `at` field.
- FR4: The server MUST retry failed jobs with backoff, and move exhausted jobs to the dead set.
- FR5: The server MUST treat an expired reservation as a failure.
- FR6: The server MUST reject a `PUSH` with a `jid` that already exists.
- FR7: The server MUST support password authentication.
- FR8: At shutdown, the server MUST stop giving out jobs and MUST accept `ACK` and `FAIL` until no job runs or `SHUTDOWN_TIMEOUT` ends. It MUST NOT send `quiet` or `terminate` for its own shutdown, because a worker that gets them stays stopped after the server returns.

### Non-functional requirements

- NFR1: After the server replies `OK`, the change MUST survive `kill -9`.
- NFR2: Memory use MUST NOT grow in proportion to the number of waiting jobs.
- NFR3: The server MUST recover without manual steps after a crash.
- NFR4: Existing Faktory clients for Go, Ruby, and Node MUST pass the core flow without changes. Bun MUST be tested separately.
- NFR5: The code, env vars, and wire protocol MUST NOT contain the project name.

### Targets to validate

These numbers are initial targets, not measurements.

- 5,000 jobs per second for push and fetch on one NVMe disk.
- Less than 256 MB of anonymous memory with 10 million waiting jobs. The OS page cache for the data file is not included.

## 4. Later phases

| Phase | Scope |
|---|---|
| 2 | Web UI. Done: HTTP API, metrics in memory, death hook, cron, queue pause, `MUTATE`, `INFO` format, worker threads and autoscale |
| 3 | Done: unique jobs, expiring jobs, throttling, batches, job tracking |
| 4 | Replication and high availability |
| 5 | SaaS: tenants, quotas, authentication with clowk, billing with contagorda-pay |

Own clients are separate repositories: `anway-go`, `anway-ruby`, and `anway-js`. See `docs/dev/clients.md`.

## 5. Out of scope for now

- Exactly-once delivery.
- Workflow engines and durable function execution.
- Clustering.
- Web UI (deferred).
- Multi-tenancy. The storage keys and connection context SHOULD NOT block it later.

## 6. Open decisions

- Do not copy code from Faktory, because Faktory is AGPL. Use only the public protocol.

- Tenant model on the wire. One option is one password per tenant.
- Replication design.
- Retention limit of the dead set.
- Retention limit of unfinished batches.
