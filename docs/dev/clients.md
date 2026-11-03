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

## Contract

The first client, `anway-go`, sets the client contract. All other clients have the same operations, argument order, defaults, and error kinds, with names in the style of each language.

The Ruby `Anway::Worker` module is the one exception. It is a job API above the Ruby client, and the client contract does not include it. It uses the client for all server calls.

## Layers

1. **Connection:** Socket, TLS, handshake, RESP, reconnect, and pool.
2. **Client:** Push, fetch, ack, fail, batch, track, queue, mutate, and info.
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
