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
| `PUBLISHB` | An array of up to 1,000 `PUBLISH` arguments | An array with one `{"event","jobs","duplicate","error"}` for each event, in the same order |

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

## Bulk publish

`PUBLISHB` sends many events in one command and one write. Use it when an app has many events at once, such as an import, an outbox relay that catches up, or a replay. One write for 1,000 events is much faster than 1,000 writes.

```text
PUBLISHB [{"topic":"order.paid","args":[1],"id":"o-1"},{"topic":"order.paid","args":[2],"id":"o-1"},{"topic":"","args":[3]}]
[{"event":"o-1","jobs":2,"duplicate":false},{"event":"o-1","jobs":0,"duplicate":true},{"event":"","jobs":0,"duplicate":false,"error":"invalid topic name: \"\""}]
```

- Each event works as one `PUBLISH`: the same fields, fan-out, dedup by id, and Dead for an event with no subscription.
- The reply has one result for each event, in the order of the events. A refused event, such as a bad topic or args that are not an array, has `error`, and it does not stop the others.
- A second event with the same topic and id in the same bulk is a duplicate of the first one.
- A bulk has at most 1,000 events, and its events can make at most 10,000 jobs over all subscriptions. A bulk above a limit is refused whole, and nothing is written.
- After the server replies, every copy of every event survives `kill -9`.

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

## HTTP API and web UI

`GET /api/v1/topics` lists each topic, by name, with its subscriptions. Each subscription has its queue, job type, `retry`, `reserve_for`, `created_at`, and `workers`: the connected workers that fetch from its queue, with `wid`, `hostname`, `pid`, `tag`, and `labels`.

The server knows the queues of a worker from its HELLO, or from its first `FETCH` when HELLO has no queues. So a Faktory client also shows.

A subscription with no worker still gets its copies, but they wait in its queue until a worker fetches from it. The web UI marks such a subscription.

The web UI Topics page shows this list. It has no counts of its own, because each copy is a job: the Jobs and Queues pages count them.

## Client requirements

A client library implements topics as part of the protocol:

1. `publish(topic, args, id:, unique_for:, custom:)` sends `PUBLISH`, and returns the event id, the jobs, and duplicate.
2. `publish_bulk(events)` sends `PUBLISHB` with up to 1,000 events, and returns one result for each event, with the reason of a refused event. More events are an error before anything goes to the server, as in a bulk push.
3. `subscribe(topic, queue, event, retry:, reserve_for:)` and `unsubscribe(topic, queue, event)` send the commands. `event` is the job type, as in the [input contract](clients.md#input-contract).
4. A worker declares its subscriptions with the `topics` key of the input contract, and sends `SUBSCRIBE` for each one when it starts, before its first `FETCH`. It fetches from the queues of its subscriptions.
5. It passes scenarios T01 to T07 in [conformance/scenarios.md](../../conformance/scenarios.md).
