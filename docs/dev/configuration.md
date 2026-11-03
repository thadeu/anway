# Configuration

Env vars do not have a product prefix, so a rename does not change deployments.

| Env var | Default | Purpose |
|---|---|---|
| `BIND_ADDR` | `127.0.0.1:7419` | Work protocol address |
| `DATA_DIR` | `./data` | Storage directory |
| `CONF_DIR` | `./conf.d` | YAML settings directory |
| `SERVER_PASSWORD` | Empty | Client password. Required when `BIND_ADDR` is not a loopback address. |
| `ALLOW_FLUSH` | `false` | Set to `true` to enable `FLUSH`. |
| `TLS_CERT_FILE` | Empty | TLS certificate. Set it with `TLS_KEY_FILE`. |
| `TLS_KEY_FILE` | Empty | TLS private key. |
| `WEB_ADDR` | Empty | HTTP API address for the web UI. Empty turns it off. |
| `WEB_TOKEN` | Empty | HTTP API token. Required when `WEB_ADDR` is not a loopback address. |
| `METRICS_RETENTION` | `24h` | Time that saved metrics stay. From `1h` to `60d`. Accepts Go durations and days, such as `30d`. |
| `BACKUP_DIR` | `$DATA_DIR/backups` | Snapshot directory. |
| `SHUTDOWN_TIMEOUT` | `30s` | Most time for running jobs to report at shutdown. The server stops sooner when no job runs. |
| `LOG_LEVEL` | `info` | Log level |

The server does not start with a public address and no password.

Use TLS when clients connect over a shared network. A password does not encrypt job data. See [tls.md](tls.md).

## Backup

`kill -USR1 <pid>` writes a consistent snapshot to `BACKUP_DIR`. The log shows `backup finished` or `backup failed`. The file appears only when it is complete.

To restore, stop the server and replace `$DATA_DIR/jobs.db` with a snapshot.

- Keep backups on a different disk. The default directory is on the data disk.
- The server does not delete old snapshots.
- A snapshot holds a long read transaction, so the data file can grow while it runs.
- Jobs after the snapshot are not in it.

## Flags

Flags have priority over env vars. The password has no flag, because a process list shows flags.

| Flag | Env var |
|---|---|
| `--bind` | `BIND_ADDR` |
| `--data-dir` | `DATA_DIR` |
| `--conf-dir` | `CONF_DIR` |
| `--tls-cert` | `TLS_CERT_FILE` |
| `--tls-key` | `TLS_KEY_FILE` |
| `--web` | `WEB_ADDR` |
| `--version` | None |
| `--help` | None |

## YAML settings

The server merges all `*.yml` and `*.yaml` files in `CONF_DIR`, in name order. `SIGHUP` reloads them. If a file is invalid, the old settings stay active. At boot, an invalid file stops the server. An unknown field is an error, so a typing error does not pass.

```yaml
throttles:
  api: { concurrency: 4, timeout: 60 }
  bulk: { worker: 5, timeout: 60 }

cron:
  - schedule: "*/5 * * * *"
    job:
      type: Cleanup
      queue: ops
      retry: 3
      args: [1]
      custom:
        expires_in: 60
```

- **Cron:** Schedules use UTC. Times missed during downtime do not run. With more than one server, each one pushes the job.
- **Throttles:** `concurrency` is a limit for all workers, and `worker` is a limit for each worker. `timeout` has a default of 60 seconds. A full throttle makes `FETCH` skip to the next queue.
