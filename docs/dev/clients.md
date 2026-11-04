# Clients

Each client is its own repository. The server repository holds these rules and the shared conformance scenarios.

All repositories are public at `github.com/thadeu`. The project is open source first.

| Repository | Content | Language floor | Package format |
|---|---|---|---|
| `anway` | Server | Go 1.27 | One binary |
| `anway-go` | Go client and worker | Go 1.26 | Module `github.com/thadeu/anway-go`, with no AGPL code |
| `anway-ruby` | Ruby client and worker, Rails integration | Ruby 3.4, Rails 8.1 | Gem `anway`, with an optional Railtie |
| `anway-js` | Client and worker for Node and Bun | Node 24, Bun 1.2 | Package `@anway/client` in a pnpm workspace, TypeScript first, ESM and CommonJS |

We support only modern language versions. There is no legacy runtime support.

## Versions

Since 0.5.1, the server and the official clients have the same version number. Each release of one repository is also a release of the others, with the same number, even when one of them has no change.

- A client and a server with the same version are tested together. The client CI and release workflows run against the server release of the same version.
- The platform gems of `anway-ruby` contain the server of the same version.
- A client also works with an older server, but a feature that needs a new command, such as `PUSHB`, needs a server that has it.
- Release the server first, because the client workflows download its binary.

## Contract

The first client, `anway-go`, sets the client contract. All other clients have the same operations, argument order, defaults, and error kinds, with names in the style of each language.

The Ruby `Anway::Worker` module is the one exception. It is a job API above the Ruby client, and the client contract does not include it. It uses the client for all server calls.

## Input contract

Since version 0.4.0, every client takes the same input names when an app declares or pushes a job. The client translates them to the fields of the protocol, so the server does not change. One table, the same in every language, is less to remember than the wire fields.

| Contract key | Wire field | Note |
|---|---|---|
| `event` | `jobtype` | The name of what the job handles. In Ruby, the default is the class name. In Go and JS, it is the first argument of `on`. |
| `queue` | `queue` | Default `default`. |
| `retry` | `retry` | Default 25 (server). `0` discards at the first failure. `-1` goes to Dead at once. |
| `reserve_for` | `reserve_for` | Seconds. |
| `backtrace` | `backtrace` | Lines. |
| `at` | `at` | A time to run later. For a push, not for a declaration. |
| `unique_for` | `custom.unique_for` | Seconds. |
| `unique_until` | `custom.unique_until` | `success` or `start`. |
| `expires_in` | `custom.expires_in` | Seconds. |
| `expires_at` | `custom.expires_at` | A time. |
| `track` | `custom.track` | `true` sends `1`. |
| `on_dead` | `custom.on_dead` | `{event, queue}`. Its `event` goes to `jobtype`. |
| `custom` | `custom` | The app's own keys only. The keys above win over a key with the same name here. |
| `topics` | One `SUBSCRIBE` for each topic | With `event` as the job type, and the `queue`, `retry`, and `reserve_for` of the declaration. For a declaration, not for a push. |

Names follow the style of each language: `reserve_for` in Ruby, `reserveFor` in JS, `ReserveFor` in Go. Durations are seconds in Ruby and JS, and `time.Duration` in Go.

**Several option sets merge in order.** A declaration can take more than one set: repeated `anway_options` calls in Ruby, or more than one options argument in Go and JS. The merge rules are the same everywhere:

- A single value, such as `queue` or `retry`: the last set that has it wins.
- `topics`: the topics of all sets add up, in order, with no duplicates.
- `custom`: the hashes merge, and a later key wins.

**The worker API:**

| Language | Declaration | Push |
|---|---|---|
| Ruby | `anway_options event:, queue:, topics:, ...` in an `Anway::Worker` class. It can be called more than once. A subclass inherits all options except `event` and `topics`. | `perform_async`, `perform_in`, `perform_at`, and `set(...)` with the same keys. `Anway::Job.new(event, args, **options)`. |
| Go | `worker.On(event, handler, options ...Options)` | `anway.NewJob(event, args, options ...Options)` |
| JS | `worker.on(event, handler, ...options)` | `new Job(event, args, options)` |

- A worker has one handler for each event. A second `on` for the same event is an error. Several flows for one topic use several events, each subscribed to the topic.
- `on` is only for events. Lifecycle hooks, if a client adds them, get their own names, such as `onError`.
- `client.subscribe` and `client.unsubscribe` take `topic`, `queue`, and `event`, with `retry` and `reserve_for` for `subscribe`.

## Layers

1. **Connection:** Socket, TLS, handshake, RESP, reconnect, and pool.
2. **Client:** Push, bulk push, fetch, ack, fail, batch, track, queue, mutate, info, and the topic commands, with bulk publish.
3. **Worker:** Concurrency, heartbeat, quiet, terminate, job registry, and middleware.
4. **Framework integration:** Rails only, for now.

## Rules

- Clients generate `jid` and `wid`.
- All clients read the server address from `ANWAY_URL`. A client runs inside an app, so this name has the product prefix to prevent a collision with an app variable.
- URLs use `tcp://` or `tcp+tls://`.
- A client passes the scenarios in `conformance/scenarios.md` before a release.
- The `anway-js` client passes on Node and on Bun.

## Worker shutdown

- `quiet` stops fetching. The worker stays alive and continues to beat.
- `terminate` or a local stop request gives running jobs a shutdown timeout, then cancels them, then gives a short grace time.
- After the grace time, the worker fails jobs that still run and stops. A late result is ignored.
- A worker does not cancel a fetch in progress, because the reply can hold a job.
- After an empty reply that returns at once, the worker waits a short time, because a quiet server replies with no delay.

## Topics

- Topics are part of the protocol. Every client library implements `publish`, `subscribe`, and `unsubscribe`, and lets a worker declare its subscriptions with the `topics` key of the [input contract](#input-contract), and send them with `SUBSCRIBE` when it starts. See [topics.md](topics.md#client-requirements).
- It passes scenarios T01 to T06.

## Worker threads

- A worker sends its thread count in HELLO and in each `BEAT` as `concurrency`, and its queues and tag in HELLO. See [worker-threads.md](worker-threads.md#client-requirements).
- When a `BEAT` reply has `concurrency`, the worker starts or stops threads. A thread that stops first finishes its job.
- A worker with a limit, such as the size of its database pool, sends it in HELLO as `max_concurrency`.
- The config can start the worker with autoscale on: `autoscale: {max}` in HELLO.

## anway-js build

Write the source in TypeScript. Use tsup, which uses esbuild, for ESM, CommonJS, and type declarations. Use pnpm and the standard ESLint rules. The public API is `Anway.Client`, `Anway.Worker`, and `Anway.Job`, from the default export.

## Rails

- The native API is a module with `perform` and `perform_async`.
- An optional ActiveJob adapter supports existing jobs.
- The server owns retries. The adapter sends `retry: -1` for jobs that use `retry_on`, so that one failure does not retry twice and the final error goes to the dead set.
- Pushes from a request use a connection pool and a timeout.

## Order

1. Conformance scenarios in this repository.
2. `anway-go`.
3. `anway-ruby`, without Rails.
4. Rails integration in `anway-ruby`.
5. `anway-js`.

Later clients: Rust, Java, Kotlin, and Swift.
