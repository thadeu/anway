# Development

## Commands

```bash
mise install
go test -race ./internal/...
go test ./test/crash
go build -o bin/server ./cmd/server && go build -o bin/loadtest ./cmd/loadtest
test/compat/run.sh
test/compat/run.sh a-password
```

`test/compat` has its own Go module, so the AGPL client is not a dependency of the server. It needs Ruby 3, Node, Bun, and installed dependencies in `test/compat/ruby` and `test/compat/node`.

## Test levels

- **Unit tests:** Protocol parsing, retry delay, and key encoding.
- **Integration tests:** A real server and a real TCP client, through each state change.
- **Compatibility tests:** Existing Faktory clients for Go, Ruby, and Node, and the Node client on Bun.
- **Crash tests:** `kill -9` under load. A confirmed job must not be lost.
- **Load tests:** Throughput, memory, and latency with a large backlog.

## Definition of done

- Tests pass.
- `docs/dev/` and `SPEC.md` match the behavior.
- The change does not add the product name to code, env vars, or the protocol.
