# Topics

A topic sends one event to many queues. `PUBLISH` makes one job for each subscription of the topic, in one transaction. Each copy is a normal job in the queue of its subscriber, with its own retries, Dead set, throttle, and autoscale. This is the SNS and SQS model.

```text
publisher               server, in one transaction                     subscribers
---------               --------------------------                     -----------
PUBLISH chat.message ─┬─> queue realtime,  job chat.broadcast ──> app A: realtime
                      ├─> queue cassandra, job chat.store     ──> app B: Cassandra
                      └─> queue push,      job chat.notify    ──> app C: push notifications
```

Use a topic when one event must go to many apps or many flows. Each subscriber has its own durable queue. If the Cassandra app is down for ten minutes, its copies wait in its queue, and the other subscribers are not blocked.

## Commands

| Command | Argument | Reply |
|---|---|---|
| `SUBSCRIBE` | `{"topic","queue","jobtype","retry","reserve_for"}` | `OK` |
| `UNSUBSCRIBE` | `{"topic","queue","jobtype"}` | `OK` |
| `PUBLISH` | `{"topic","args","id","unique_for","custom"}` | `{"event","jobs","duplicate"}` |

Topic names follow the queue name rule: letters, digits, `.`, `_`, and `-`, up to 255 characters. A topic exists as soon as a subscription or an event uses it.

## Subscriptions

A subscription is a topic, a queue, and a job type. The server saves it on disk, so it does not depend on a connected worker. A copy goes to the queue even while the subscriber app is down.

- `SUBSCRIBE` is idempotent. The same topic, queue, and job type keep one subscription, and a second `SUBSCRIBE` sets its new `retry` and `reserve_for`. A client library sends it each time a worker starts.
- `UNSUBSCRIBE` removes a subscription. A missing subscription is not an error.
- `retry` and `reserve_for` are the options of each copy, as in a job.
- A topic has at most 256 subscriptions.
- One app can subscribe two job types to the same topic, for two flows.
- An event that is published before a subscription exists does not reach that subscription. This is the same as in SNS.

## Publish

```text
PUBLISH {"topic":"chat.message","args":[{"room":"general","body":"hi"}],"id":"msg-42"}
{"event":"msg-42","jobs":3,"duplicate":false}
```

- `args` is an array, as in a job. Every copy gets the same args.
- `id` is the event id. With an id, a second `PUBLISH` of the same topic and id within `unique_for` seconds does nothing, and its reply has `"duplicate": true`. `unique_for` has a default of 86,400 seconds (one day), a maximum of 30 days, and 0 turns the check off. Without an id, the server makes one, such as `evt-…`, and there is no check.
- `custom` goes to each copy. The server adds `topic` and `event` to it, so a handler knows which event it runs.
- Each copy has a new jid, the queue, job type, `retry`, and `reserve_for` of its subscription, and the args of the event.
- The reply has `jobs`: the number of copies.

The copies are written in one transaction. After the server replies, every copy survives `kill -9`, and a crash before the reply leaves no copy.

## An event with no subscription

An event with no subscription is not lost. It goes to the Dead set as one job:

| Field | Value |
|---|---|
| `jobtype` | The topic name |
| `queue` | `topic` |
| `custom` | `topic`, `event`, and `unrouted: true` |
| `failure` | `NoSubscriber`: "no subscription for topic …" |

The reply has `"jobs": 0`. A requeue of this job, with `MUTATE` or the web UI, publishes the event again to the subscriptions of that moment, and removes the dead job. With still no subscription, the event stays in Dead. A kill or a discard works as for any dead job.

## Delivery

- Each copy is delivered at least once, as any job. Handlers must be idempotent. `custom.event` is a good key for that.
- Copies in different queues have no order between them. A queue keeps its own order.
- There are no filters. Each subscriber gets every event of the topic, and skips the ones it does not need in its own code.

## Client requirements

A client library implements topics as part of the protocol:

1. `publish(topic, args, id:, unique_for:, custom:)` sends `PUBLISH`, and returns the event id, the jobs, and duplicate.
2. `subscribe(topic, queue, jobtype, retry:, reserve_for:)` and `unsubscribe(topic, queue, jobtype)` send the commands.
3. A worker can declare its subscriptions, for example in a job class, and send `SUBSCRIBE` for each one when it starts, before its first `FETCH`. It fetches from the queues of its subscriptions.
4. It passes scenarios T01 to T06 in [conformance/scenarios.md](../../conformance/scenarios.md).
