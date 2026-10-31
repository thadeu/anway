# Architecture

```text
Producers (Rails, Go, Node, Bun)        Workers (any language)
             |                                   ^
             | PUSH                              | FETCH / ACK / FAIL / BEAT
             v                                   v
+--------------------------------------------------------------+
|                         TCP server                           |
|                 connection and protocol layer                |
+--------------------------------------------------------------+
|                         Job manager                          |
|       state changes, blocked fetch waiters, worker table     |
+----------------------------+---------------------------------+
|  Background loops          |  Storage                        |
|  scheduler, retries,       |  embedded B+tree (bbolt)        |
|  reservation reaper        |  on local disk                  |
+----------------------------+---------------------------------+
```

## Components

- **Protocol layer:** Reads commands, writes RESP replies, and does authentication. It has no job logic.
- **Job manager:** The only component that changes job state. Each change is one atomic batch.
- **Storage:** A small interface over bbolt. Other components do not use bbolt directly.
- **Background loops:** Each loop runs about once per second. They move due jobs to their queues and expire reservations.
- **Worker table:** In-memory data about connected workers and their last `BEAT`. It is not persisted.

## Main decisions

| Decision | Reason |
|---|---|
| Go | One static binary, and good concurrency for network servers. |
| Embedded bbolt storage | Data stays on disk, there is no external service, and queue deletes leave no tombstones. |
| Faktory protocol | Clients for all target languages exist on day 1. |
| Reservations on disk | A restart does not lose jobs that are in progress. |
| At-least-once delivery | Exactly-once is not possible across a worker crash. Job handlers must be idempotent. |

## Layout

```text
cmd/server/        Entry point
internal/protocol/ RESP and command parsing
internal/server/   TCP connections
internal/manager/  Job state changes
internal/storage/  Storage interface and bbolt implementation
internal/cron/     YAML settings and periodic jobs
internal/config/   Configuration, and the only location of the product name
internal/testclient/ Minimal client for tests and load tools
cmd/loadtest/      Load tool
test/crash/        SIGKILL tests
test/compat/       Tests with real clients, in their own modules
```
