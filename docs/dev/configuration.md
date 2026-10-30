# Configuration

Env vars do not have a product prefix, so a rename does not change deployments.

| Env var | Default | Purpose |
|---|---|---|
| `BIND_ADDR` | `0.0.0.0:7419` | Work protocol address |
| `DATA_DIR` | `./data` | Storage directory |
| `CONF_DIR` | `./conf.d` | TOML settings directory |
| `SERVER_PASSWORD` | Empty | Client password. Empty disables authentication. |
| `SHUTDOWN_TIMEOUT` | `30s` | Time for workers to finish |
| `LOG_LEVEL` | `info` | Log level |

Set `SERVER_PASSWORD` in all environments outside local development.

## TOML settings

The server merges all `*.toml` files in `CONF_DIR`. `SIGHUP` reloads them. If a file is invalid, the old settings stay active. At boot, an invalid file stops the server.

```toml
[throttles]
api = { concurrency = 4, timeout = 60 }
bulk = { worker = 5, timeout = 60 }

[[cron]]
  schedule = "*/5 * * * *"
  [cron.job]
    type = "Cleanup"
    queue = "ops"
    retry = 3
    args = [1]
    [cron.job.custom]
      expires_in = 60
```

- **Cron:** Schedules use UTC. Times missed during downtime do not run. With more than one server, each one pushes the job.
- **Throttles:** `concurrency` is a limit for all workers, and `worker` is a limit for each worker. `timeout` has a default of 60 seconds. A full throttle makes `FETCH` skip to the next queue.
