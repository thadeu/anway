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
| GET | `/api/v1/metrics?range=` or `?from=&until=` | Done and failed jobs, and the peak of worker threads. See Metrics. |
| GET | `/api/v1/queues` | Size, paused, oldest job age, jobs per minute, time to clear |
| POST | `/api/v1/queues/{name}/pause` | Pause |
| POST | `/api/v1/queues/{name}/resume` | Resume |
| DELETE | `/api/v1/queues/{name}/jobs` | Delete the waiting jobs. The paused state stays. |
| GET | `/api/v1/jobs?set=&limit=&cursor=&queue=&jobtype=&worker=` | One page of jobs |
| GET | `/api/v1/jobs/groups?set=` | Jobs grouped by cause |
| POST | `/api/v1/jobs/actions` | Requeue, kill, or discard jobs |
| POST | `/api/v1/jobs` | Enqueue a job. The body is a job payload. |
| GET | `/api/v1/workers` | Worker processes: hostname, pid, tag, queues, threads, busy count, start time, last beat |
| PATCH | `/api/v1/workers/{wid}` | Change the threads of a worker, or its autoscale. See [Workers](#workers). |
| GET | `/api/v1/schedules` | Cron entries with the next run, and throttles in use |
| GET | `/api/v1/batches` | Batches, up to 200 |

Errors are `{"error":"..."}` with status 400, 401, 409, or 500. A unique lock conflict is 409.

## Jobs

`set` is `retries`, `scheduled`, `dead`, `working`, `waiting`, or `queue:<name>`. `working` holds the running jobs. `waiting` holds the waiting jobs of all queues, oldest first by enqueue time. `queue:<name>` holds the waiting jobs of one queue, in queue order. `limit` is 1 to 200, and the default is 50. `next_cursor` is blank on the last page. Each page is one short read transaction.

Filters are optional, and an empty filter matches all jobs:

| Filter | Matches |
|---|---|
| `queue` | The queue name, exactly |
| `jobtype` | A part of the job type, or of `custom.wrapped`, the class that a wrapper job such as an Active Job job runs |
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

## Workers

Each worker has `concurrency` (the threads it runs), `wanted` (the count that it gets at its next beat, or null), `scalable` (true when it can change its threads), `max_concurrency` (its limit, or null), and `autoscale` (`{"min","max"}`, or null).

`PATCH /api/v1/workers/{wid}` takes `concurrency`, `autoscale`, or both:

```json
{"concurrency": 12}
{"autoscale": {"max": 40}}
{"autoscale": {"min": 4, "max": 40}}
{"autoscale": false}
{"reset": true}
```

The reply is the worker. The worker gets the change at its next beat, up to 15 seconds later. The server saves the change for the worker name (hostname and tag), so a new process with that name starts with it, also after a restart. `{"reset": true}`, alone in the body, drops the saved setting and brings back the config of the worker. Each worker has `saved`: true when it runs on a saved setting. A missing `min` is the count that the worker started with. With autoscale on, `concurrency` must be between `min` and `max`.

| Status | When |
|---|---|
| 400 | A count outside 1 to 1000 or above `max_concurrency`, `min` above `max`, a body with no field, or `reset` with another field |
| 404 | No worker with this `wid` |
| 409 | The worker does not report its threads, for example a Faktory client |

[worker-threads.md](worker-threads.md) has the autoscale rule and an example.

## Metrics

| Range | Points | Source |
|---|---|---|
| `60s` | 60, one per second | Memory only |
| `15m`, `1h` | One per minute | Memory |
| `24h` | One per 10 minutes | Memory |
| `7d`, `30d`, `60d` | One per hour, 4 hours, and 8 hours | Saved minutes |

`?from=2026-11-03T14:10:00Z&until=2026-11-03T14:25:00Z` returns a custom window, such as a slice that a user drags on a chart. Both ends are RFC 3339 times, and the window is at most 60 days. The step is the smallest of 1, 5, 10, 15, 30, 60, 120, 240, 480, 720, or 1,440 minutes that gives at most 120 points. Minutes of the last 24 hours come from memory, and older minutes from the saved minutes. The reply has `"range": "custom"`, `from`, and `until`.

Each point has `time`, `done`, `failed`, and `threads`. `done` and `failed` add up over the step. `threads` is the peak of the total threads of the connected workers in the step: the server reads it each second and keeps the highest value of each minute. A chart can then show when autoscale raised the threads. The `60s` range has no thread peak, and minutes saved before this field existed read as 0.

The last point is the current minute or second, and it grows while the period runs. `overview.rate` is the average per second of the last 5 complete seconds.

The server saves each changed minute once a minute and at shutdown, and loads the last 24 hours at boot. After SIGKILL, only the current minute is lost. Seconds are not saved. `METRICS_RETENTION` sets how long saved minutes stay, from 1 hour to 60 days.

## Limits

- Jobs per minute for a queue is the average of the last 5 complete minutes.
- Threads come from the optional `concurrency` field in `HELLO`. A client that does not send it counts as 0.
