# Protocol

The protocol follows the [public Faktory work protocol](https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md), version 2. If this page and that specification do not agree, the specification is correct. Confirm exact bytes against it and against real clients.

## Transport

- TCP. The default port is 7419.
- A client sends one CRLF-terminated line: a verb, then optional arguments.
- Replies use Redis RESP. Text is UTF-8.
- One command runs at a time on a connection. There is no pipelining.

## Handshake

1. The server sends `HI` with JSON: `v`, and also `i` and `s` if a password is set.
2. The client sends `HELLO` with JSON: `v`, and `pwdhash` if necessary.
3. A worker also sends `hostname`, `wid`, `pid`, and `labels`. It can also send these Anway extensions, which the web UI shows:
   - `concurrency`: the number of jobs it runs at once, shown as threads.
   - `tag`: a short name for the app or the role of the process, such as `shop`. Sidekiq calls this the tag.
   - `queues`: the queues that it fetches, in order.
   - `autoscale`: `{"max": n}` or `{"min": n, "max": n}`, to start with autoscale on. Min is `concurrency` by default. See [Worker threads](#worker-threads).
   - `max_concurrency`: the most threads that the worker can run, such as the size of its database pool. The server never asks for more, and lowers an autoscale max to it.
4. The server replies `OK`.

`pwdhash` is the hex result of SHA-256 on the password plus the salt, applied `i` times.

## Commands

| Command | Argument | Reply |
|---|---|---|
| `PUSH` | Job JSON | `OK`, or an error |
| `FETCH` | Queue names | Job JSON, or null after a maximum of 2 seconds |
| `ACK` | `{"jid"}` | `OK` |
| `FAIL` | `{"jid","errtype","message","backtrace"}` | `OK` |
| `BEAT` | `{"wid","current_state","rss_kb","concurrency"}`. The web UI shows `current_state`. `concurrency` is an Anway extension: see [Worker threads](#worker-threads). | `OK`, or JSON with `state` (`quiet` or `terminate`), `concurrency`, or both |
| `INFO` | None | Server data as JSON, with the `server` and `faktory` hashes |
| `QUEUE` | `PAUSE`, `RESUME`, or `REMOVE`, then queue names or `*` | `OK` |
| `MUTATE` | `{"cmd","target","filter"}` | `OK` |
| `BATCH NEW` | `{"parent_bid","description","success","complete"}` | The `bid` |
| `BATCH OPEN` | `bid` | The `bid` |
| `BATCH COMMIT` | `bid` | `OK` |
| `BATCH STATUS` | `bid` | Status JSON |
| `TRACK GET` | `jid` | Track JSON |
| `TRACK SET` | `{"jid","percent","desc","reserve_until"}` | `OK` |
| `FLUSH` | None | `OK`. It deletes all data. Disabled unless `ALLOW_FLUSH=true`. |
| `END` | None | `OK`, then the connection closes |

## Worker threads

These Anway extensions let the server change the threads of a worker. [worker-threads.md](worker-threads.md) explains the feature and the autoscale rule.

1. A worker that can change its threads sends its current count in each `BEAT` as `concurrency`. A worker that does not send it never gets a change, so Faktory clients work as before.
2. When the wanted count differs from the reported count, the `BEAT` reply has `{"concurrency": n}`. It has `state` only for `quiet` or `terminate`, so a Faktory client that reads only `state` is not affected.
3. The worker starts or stops threads. A thread that stops first finishes its job. It sends a `BEAT` with the new count at once. The server replies `OK`, and does not run autoscale on a `BEAT` that only confirms a change.

A worker with a limit, such as the size of its database pool, sends it as `max_concurrency` in HELLO. The server refuses a count or an autoscale max above it.

## Job payload

| Field | Required | Default | Note |
|---|---|---|---|
| `jid` | Yes | | Globally unique |
| `jobtype` | Yes | | Tells the worker what to run |
| `args` | Yes | | JSON array. `null` becomes `[]`, because the Go client sends it for jobs with no args. |
| `queue` | No | `default` | |
| `reserve_for` | No | 1800 | Seconds. Minimum 60. |
| `at` | No | Now | RFC3339 |
| `retry` | No | 25 | 0 discards a failed job. -1 sends it to the dead set. |
| `backtrace` | No | 0 | Lines to keep |
| `created_at` | No | Server time | |
| `custom` | No | | JSON hash |
| `enqueued_at` | Read-only | | Set by the server |
| `failure` | Read-only | | Set by the server |

A `PUSH` that a unique lock blocks returns `-NOTUNIQUE Job not unique`.

## MUTATE

- `cmd`: `kill`, `discard`, `requeue`, or `clear`.
- `target`: `retries`, `scheduled`, or `dead`.
- `filter`: optional `jobtype`, `jids`, and `regexp`. Despite its name, `regexp` is a glob with `*` and `?`, matched against the job JSON.

## Custom fields

| Field | Effect |
|---|---|
| `expires_at` | RFC3339 time. A job past this time is discarded at fetch. |
| `expires_in` | Seconds. The server converts it to `expires_at` at push. |
| `unique_for` | Seconds. Blocks jobs with the same type, queue, and args. |
| `unique_until` | `success` (default) or `start`. Tells when the lock is released. |
| `on_dead` | `{"jobtype","queue"}`. When the job dies, the server pushes this job. See [job-lifecycle.md](job-lifecycle.md). |
| `bid` | Adds the job to an open batch. |
| `track` | `1` enables tracking. |
| `_bid`, `_cb` | Set by the server on callback jobs. |

See [batches.md](batches.md).

## Extensions

New commands and optional fields only. Do not change the behavior of existing commands.
