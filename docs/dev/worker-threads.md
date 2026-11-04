# Worker Threads and Autoscale

A worker process runs a number of jobs at once: threads in Ruby, goroutines in Go, promises in JS. This page calls them threads. The server can change this number while the worker runs, from the web UI, from the HTTP API, or with autoscale.

## Overview

```text
 web UI / HTTP API                server                         worker
 -----------------                ------                         ------
 PATCH concurrency=12  ------->   wanted = 12
                                                <-------------   BEAT {"concurrency": 8}
                                  wanted != 8   ------------->   {"concurrency": 12}
                                                                 starts 4 threads
                                                <-------------   BEAT {"concurrency": 12}
                                  wanted = done ------------->   OK
```

- The server holds the count that each worker must run. The worker only obeys. Because of this, all clients share one autoscale rule.
- The change travels in the `BEAT` reply. It reaches the worker at its next beat, up to 15 seconds later.
- The worker starts threads at once. A thread that must stop first finishes its job. The worker never stops a running job to scale down.
- The server saves the change for the worker **name**: its hostname and its tag. A new process with that name, for example after a restart or a deploy, starts with the saved setting, not with its config. A reset drops the saved setting. See [Saved settings](#saved-settings).

## Which workers can scale

A worker can scale when it sends its thread count in each `BEAT`. The web UI calls this worker scalable. Other workers, such as Faktory clients, never get a change, and the API answers `409` for them.

| Field | Where | Meaning |
|---|---|---|
| `concurrency` | HELLO and BEAT | The threads that the worker runs now |
| `max_concurrency` | HELLO | The most threads that the worker can run, such as the size of its database pool |
| `autoscale` | HELLO | `{"max": n}` or `{"min": n, "max": n}`, to start with autoscale on |

The wire format is in [protocol.md](protocol.md#worker-threads).

## Manual count

```bash
curl -X PATCH http://127.0.0.1:7420/api/v1/workers/$WID \
  -H "Authorization: Bearer $WEB_TOKEN" \
  -d '{"concurrency": 12}'
```

- The count is 1 to 1000, and at most `max_concurrency` when the worker sends it.
- While autoscale is on, the count must be between `min` and `max`. Autoscale then continues from that count.
- The reply is the worker. Its `wanted` field holds the count until the worker reports it. Then `wanted` is null.

## Autoscale

```bash
curl -X PATCH http://127.0.0.1:7420/api/v1/workers/$WID \
  -H "Authorization: Bearer $WEB_TOKEN" \
  -d '{"autoscale": {"max": 40}}'
```

- `max` is required. `min` is the count that the worker started with, unless you set it.
- `max` cannot be above `max_concurrency`. A HELLO `autoscale` max above it goes down to it.
- `{"autoscale": false}` turns autoscale off. The worker keeps the count that it has.

At each beat of the worker, the server reads two values:

- **Backlog:** the jobs that wait in the queues of this worker. Jobs in other queues do not count.
- **Busy:** the jobs that this worker runs now.

Then it applies the first rule that matches:

| Condition | Change |
|---|---|
| The count is above `max` or below `min` | Go to `max` or `min` |
| Burst: at least 2 waiting jobs for each thread, and all threads busy | Double the threads and add 15% more, up to `max` |
| Backlog above 0, and all threads busy | Add a quarter of the threads, at least 1, up to `max` |
| Backlog 0, and at most half of the threads busy, for 60 seconds | Remove a quarter of the threads, at least 1, down to `min` |
| Anything else | No change. The 60 seconds start again. |

The server adds threads fast and removes them slowly. A short pause in the jobs does not remove threads, and the next burst does not start from a small count.

### Example

A worker starts with `concurrency: 10` and `autoscale: {max: 40}`, and beats every 15 seconds. A burst of 5,000 jobs arrives:

| Beat | Threads | Busy | Backlog | Next count |
|---|---|---|---|---|
| 1 | 10 | 10 | 5,000 | 22 (burst: 10 + 10 + 15%) |
| 2 | 22 | 22 | 4,700 | 40 (burst, stops at max) |

The worker reaches its max in about 30 seconds. With a small backlog, under 2 waiting jobs for each thread, a step adds only a quarter, so a few waiting jobs do not double the threads. After the backlog is done, it goes down 40, 30, 22, 16, 12, 10, with one step each minute at low load. This takes about 5 minutes.

### Things to know

- The backlog includes paused queues. A paused queue alone does not add threads, because its jobs do not keep threads busy. But when the worker is busy with its other queues, the paused backlog counts. Pause a queue for a long time only on workers with autoscale off, or take it out of their queues.
- Each worker scales by itself. When many workers share a queue, each one can add threads for the same backlog. Set `max` for each worker with this in mind.
- More threads help only when the jobs wait on IO, such as HTTP calls or the database. Jobs that use the CPU do not go faster with more threads than cores.
- Each thread can hold a database connection. A worker must send its pool size as `max_concurrency`, or set `max` at or below it. Otherwise jobs can fail with a pool timeout.
- A saved setting survives a server restart and a `kill -9`. It belongs to the worker name, so a container with a random hostname gets a new name at each deploy. Set a stable hostname, such as `ANWAY_HOSTNAME=worker.1`, to keep the setting across deploys.

## Saved settings

A change from the web UI or the API is an operator decision, so it wins over the config of the worker, also after a restart:

1. The server saves the change on disk for the worker name: hostname and tag, such as `mac.local` and `shop`. The pid and the wid change at each start, and the name does not.
2. When a process with that name connects, the server applies the saved setting in place of the `autoscale` and `concurrency` from HELLO. The worker gets the count at its first beat.
3. A reset (`{"reset": true}`, or **Reset to config** in the web UI) drops the saved setting. The running worker goes back to its HELLO values, and later processes use their config again.

What is saved:

| The operator set | Saved | A new process gets |
|---|---|---|
| Autoscale with a min and a max | The range | That range, in place of the config |
| A thread count, with autoscale off | The count, and autoscale off | That count at its first beat |
| Autoscale off, with no count | Autoscale off | Its config count, with autoscale off |

- A saved count or max above the limit of the new process, such as a smaller database pool, goes down to that limit.
- A change applies to the process that you changed, and is saved for its name. Other running processes with the same name get it when they restart.
- A worker with no hostname has no name. Its change lasts until the process stops.

## Web UI

The Workers page shows one bar for each worker:

- Green: busy threads.
- Grey: free threads.
- Dashed: the room that autoscale can still add, up to `max`.
- Amber mark: a count that the worker gets at its next beat.

The Overview throughput chart has a Threads line on its own axis, at the right. Each step shows the peak of the total threads of the connected workers, so you can see when autoscale raised them. The server keeps these peaks with the other metrics, for up to `METRICS_RETENTION`.

**Scale** opens a dialog. It sets the count, or turns autoscale on with a min and a max. The dialog shows the limit of the worker when it sends one. A worker on a saved setting shows "saved in console", and its dialog has **Reset to config**.

## Client requirements

A client that supports scaling must:

1. Send `concurrency` in HELLO and in each `BEAT`. Send it in the first `BEAT` right after start, so the web UI can scale the worker at once. After a change, send a `BEAT` with the new count at once, not at the next interval. The server does not run autoscale on that `BEAT`, so one change is not stacked on another.
2. Send `max_concurrency` in HELLO when it has a limit, such as a database pool.
3. On a `BEAT` reply with `concurrency`, start or stop threads. A thread that stops first finishes its job. Cancel a pending stop before it starts new threads.
4. Make its connection pool large enough for the new threads.
5. Read `state` and `concurrency` apart. A reply can have one, the other, or both.
6. Pass scenario W12 in [conformance/scenarios.md](../../conformance/scenarios.md).

`anway-ruby`, `anway-go`, and `anway-js` support scaling. In Go a thread is a goroutine. In Node and Bun it is a job slot: one concurrent fetch-and-run loop in the event loop, which helps jobs that wait on I/O, not jobs that use the CPU.
