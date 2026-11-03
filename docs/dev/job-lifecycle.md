# Job Lifecycle

```text
PUSH --> SCHEDULED (future "at")
  |          |
  |          v  time arrives
  +----> ENQUEUED <-------------- RETRIES
             |                       ^  |
             | FETCH                 |  | no retries remain
             v                       |  v
          WORKING ---- FAIL or ------+  DEAD
             |         reservation
             | ACK     expires
             v
          removed
```

## Rules

- `ACK` removes the job.
- `ACK` or `FAIL` for a job that is not in WORKING returns an error. This can occur after a reservation expires.
- `FAIL` and an expired reservation follow the same path.
- At `retry: 0`, a failed job is discarded. At `retry: -1`, it goes directly to DEAD.
- Delivery is at least once. A worker can receive a job again, so handlers must be idempotent.
- The first fetch has no `failure` field. `retry_count` starts at 0 at the first failure.

## Retry delay

```text
delay_seconds = count^4 + 15 + random(0..30) * (count + 1)
```

`count` is the number of failures so far. With 25 retries, the total time is about 3 weeks. The formula is a project choice. Tune it after real use.

The `failure` field records the retry count, the failure time, the next retry time, the error type, the message, and the backtrace.

## Worker shutdown

1. The server sends `quiet` in the `BEAT` reply. The worker stops fetching.
2. The server sends `terminate`. The worker fails its running jobs within 30 seconds, then ends.
3. Jobs that do not receive `ACK` or `FAIL` return through the reservation timeout.

## Death hook

A job with `custom.on_dead` names a job that the server pushes when the job dies:

```json
{"jobtype": "mail.deliver", "args": [42], "retry": 15,
 "custom": {"on_dead": {"jobtype": "mail.bounced", "queue": "rails"}}}
```

- The notice goes to its queue in the same transaction as the death, so a crash cannot lose it.
- Its one arg is a hash with `jid`, `jobtype`, `queue`, `args`, `failure`, and `custom` of the dead job.
- It fires when the retries end, at `retry: -1`, and when the last reservation expires.
- It does not fire for a manual kill with `MUTATE` or the HTTP API, or for a discard. This includes the kill of a running job.
- The notice is a normal job with the default retries. It has no hook of its own.
