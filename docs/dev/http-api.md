# HTTP API

The web UI uses this API. Job traffic never uses it: producers and workers use the TCP protocol, and the API can be off without effect on jobs.

The API is off by default. Set `WEB_ADDR`, for example `127.0.0.1:7420`. A non-loopback address needs `WEB_TOKEN`.

## Authentication

Routes under `/api` need `Authorization: Bearer <WEB_TOKEN>` when a token is set. `GET /healthz` needs no token.

Do not give the token to a browser. A proxy, such as the web UI image or the Rails mount, holds it.

## Routes

| Method | Path | Result |
|---|---|---|
| GET | `/healthz` | `{"status":"ok"}` |
| GET | `/api/v1/overview` | Server, job states, totals, threads, live rate per second, workers, queues, paused queues |
| GET | `/api/v1/metrics?range=` | Done and failed jobs. See Metrics. |
| GET | `/api/v1/queues` | Size, paused, oldest job age, jobs per minute, time to clear |
| POST | `/api/v1/queues/{name}/pause` | Pause |
| POST | `/api/v1/queues/{name}/resume` | Resume |
| DELETE | `/api/v1/queues/{name}/jobs` | Delete the waiting jobs. The paused state stays. |
| GET | `/api/v1/jobs?set=&limit=&cursor=&queue=&jobtype=&worker=` | One page of jobs |
| GET | `/api/v1/jobs/groups?set=` | Jobs grouped by cause |
| POST | `/api/v1/jobs/actions` | Requeue, kill, or discard jobs |
| POST | `/api/v1/jobs` | Enqueue a job. The body is a job payload. |
| GET | `/api/v1/workers` | Worker processes, threads, busy count, last beat |
| GET | `/api/v1/schedules` | Cron entries with the next run, and throttles in use |
| GET | `/api/v1/batches` | Batches, up to 200 |

Errors are `{"error":"..."}` with status 400, 401, 409, or 500. A unique lock conflict is 409.

## Jobs

`set` is `retries`, `scheduled`, `dead`, `working`, `waiting`, or `queue:<name>`. `working` holds the running jobs. `waiting` holds the waiting jobs of all queues, oldest first by enqueue time. `queue:<name>` holds the waiting jobs of one queue, in queue order. `limit` is 1 to 200, and the default is 50. `next_cursor` is blank on the last page. Each page is one short read transaction.

Filters are optional, and an empty filter matches all jobs:

| Filter | Matches |
|---|---|
| `queue` | The queue name, exactly |
| `jobtype` | A part of the job type |
| `worker` | The worker id that runs the job, exactly |

With a filter, one page reads at most 5,000 jobs. So a page can have fewer jobs than `limit`, or none, and still have a `next_cursor`. Read pages until `next_cursor` is blank.

Each job has `group`, the same value that `/jobs/groups` returns. A waiting job has `enqueued_at`. A running job has `worker` and `started_at`, and `at` is the end of its reservation.

## Groups

A group is the job type plus the error type and message, with numbers and long hex ids removed. So `timeout after 30s` and `timeout after 31s` are one group. The scan stops at 10,000 jobs and then returns `"truncated": true`.

## Actions

```json
{"action": "requeue", "set": "retries", "group": "9f2ca17e04d1"}
{"action": "kill", "set": "retries", "jids": ["a", "b"]}
{"action": "discard", "set": "dead", "all": true}
{"action": "kill", "set": "working", "all": true, "queue": "media"}
```

`action` is `requeue`, `kill`, or `discard`. `set` is `retries`, `scheduled`, `dead`, or `working`. A request needs `jids`, `group`, or `all`, so that an empty body never changes every job. The optional filters `queue`, `jobtype`, and `worker` limit the change, the same as in the job list. The result is `{"changed": n}`.

### Kill a running job

`working` accepts only `kill`. The job goes to the dead set with the failure `Killed: Killed while it ran`, and its throttle lease is released. In a batch, it counts as failed.

The server does not stop the worker process. The worker can still run the code to its end, but its `ACK` or `FAIL` for the job then fails with `job is not reserved`. Use kill to free a stuck reservation, or to stop a job that must not be retried. The protocol `MUTATE` command does not accept `working`.

## Metrics

| Range | Points | Source |
|---|---|---|
| `60s` | 60, one per second | Memory only |
| `15m`, `1h` | One per minute | Memory |
| `24h` | One per 10 minutes | Memory |
| `7d`, `30d`, `60d` | One per hour, 4 hours, and 8 hours | Saved minutes |

The last point is the current minute or second, and it grows while the period runs. `overview.rate` is the average per second of the last 5 complete seconds.

The server saves each changed minute once a minute and at shutdown, and loads the last 24 hours at boot. After SIGKILL, only the current minute is lost. Seconds are not saved. `METRICS_RETENTION` sets how long saved minutes stay, from 1 hour to 60 days.

## Limits

- Jobs per minute for a queue is the average of the last 5 complete minutes.
- Threads come from the optional `concurrency` field in `HELLO`. A client that does not send it counts as 0.
