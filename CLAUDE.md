# Project Instructions

Language-agnostic background job server in Go, with embedded disk storage. Read `SPEC.md` first, then `docs/dev/`.

## Rules

- **Name:** The product is Anway, by Clowk. Server env vars have no product prefix, because the server runs alone. The name stays in one config location in the server code.
- **Client env:** Clients read `ANWAY_URL`. They run inside apps, and a generic name can collide with an app variable.
- **Protocol:** Stay compatible with the public Faktory work protocol. Add new features only as new commands or optional fields.
- **Legal:** Do not copy or read for reuse any Faktory source code. Use only the public protocol specification and observed behavior.
- **Durability:** Reply `OK` only after a synced write. Each state change is one atomic storage batch.
- **Memory:** Do not keep job data in memory in proportion to the backlog.
- **Dependencies:** Keep few dependencies. Discuss a new dependency before you add it.
- **Scope:** Implement only the current phase in `SPEC.md`.

## Versions and releases

The server and all its children have one version number. A version bump of the server is a bump of every child, in the same release, also when a child has no change. Never release the server alone.

| Child | Where the version is |
|---|---|
| `anway-go` | The git tag `vX.Y.Z`, and `SERVER_VERSION: vX.Y.Z` in `.github/workflows/ci.yml` |
| `anway-ruby` | `VERSION` and `SERVER_VERSION` in `lib/anway/version.rb`, then the tag |
| `anway-js` | `version` in `packages/client/package.json`, and `SERVER_VERSION: vX.Y.Z` in `.github/workflows/ci.yml` and `release.yml`, then the tag |
| `anway-webui` | The git tag, and `ANWAY_VERSION` in `.github/workflows/test.yml` |
| `anway-lp` | `version` in `lib/site.ts`, "The current release" in `content/docs/reference/versions.mdx`, and each "Since X.Y.Z" of the new parts |
| `anway-cloud` | `version` in `config/cloud.yml` (the floor), and the images in `.voodu/jobs.voodu` and `.voodu/web.voodu` (`anway-webui`) |

- Each child tests against the server release of the same version. Before that release exists, test against a server built from the change (`SERVER_BIN`).
- Release order: the server tag first, and wait for its release and image. Then `anway-go`, `anway-ruby` and `anway-js` (each one: push `main`, wait for a green CI, then the tag, because a published version cannot be used again). Then `anway-webui`. Then `anway-lp` with a `tag(patch):` commit, which deploys the site. Then `anway-cloud`.
- A new command or field in the server needs the same change in every client: see `docs/dev/clients.md` and `conformance/scenarios.md`.
- `anway-cloud`: a customer upgrades a server from the Cloud web ("Check for updates" finds the newest release whose image exists), with no Cloud deploy. The Cloud deploy is still needed for its console image (`anway-webui`) and the floor in `config/cloud.yml`.
- `anway-webui` runs one image in front of servers of older versions (Anway Cloud upgrades a server only when its owner asks). A page that uses a new server field or route keeps working with the previous server version: it hides what needs the new server and says which version it needs.

## Documents

- `SPEC.md`: What to build. Update it when a requirement changes.
- `docs/dev/`: Official documentation. Update it in the same change as the code.
- `.plans/`: Local plans, ignored by git. Name files `NNN-topic.md`.

## Code style

- Run `gofmt`, `go vet`, and the tests before you finish a change.
- Use one blank line between blocks with different responsibilities: before `if`, `for`, `switch`, `select`, `defer`, `go`, and `return`, and after a closing `}` when more statements follow.
- Keep `if err != nil` directly under the call that produced `err`.
- Do not skip crash tests for changes to storage or the job lifecycle.
