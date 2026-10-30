# Project Instructions

Language-agnostic background job server in Go, with embedded disk storage. Read `SPEC.md` first, then `docs/dev/`.

## Rules

- **Temporary name:** Do not put the project name in env vars, identifiers, protocol strings, or log messages. Env vars have no product prefix. The name stays in one config location.
- **Protocol:** Stay compatible with the public Faktory work protocol. Add new features only as new commands or optional fields.
- **Legal:** Do not copy or read for reuse any Faktory source code. Use only the public protocol specification and observed behavior.
- **Durability:** Reply `OK` only after a synced write. Each state change is one atomic storage batch.
- **Memory:** Do not keep job data in memory in proportion to the backlog.
- **Dependencies:** Keep few dependencies. Discuss a new dependency before you add it.
- **Scope:** Implement only the current phase in `SPEC.md`.

## Documents

- `SPEC.md`: What to build. Update it when a requirement changes.
- `docs/dev/`: Official documentation. Update it in the same change as the code.
- `.plans/`: Local plans, ignored by git. Name files `NNN-topic.md`.

## Code style

- Run `gofmt`, `go vet`, and the tests before you finish a change.
- Use one blank line between blocks with different responsibilities: before `if`, `for`, `switch`, `select`, `defer`, `go`, and `return`, and after a closing `}` when more statements follow.
- Keep `if err != nil` directly under the call that produced `err`.
- Do not skip crash tests for changes to storage or the job lifecycle.
