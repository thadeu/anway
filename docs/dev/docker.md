# Docker

Each `v*` tag publishes `ghcr.io/thadeu/anway` for `linux/amd64` and `linux/arm64`. Tags are `latest`, the full version, `major.minor`, and `major`.

```bash
docker run -d --name anway \
  -p 7419:7419 \
  -e SERVER_PASSWORD=change-me \
  -v anway-data:/data \
  --stop-timeout 35 \
  ghcr.io/thadeu/anway:latest
```

- `SERVER_PASSWORD` is required, because the image binds to `0.0.0.0`.
- Keep `/data` on a volume. Without it, a new container has no jobs.
- The process runs as UID 65532. A bind mount on `/data` must allow this user to write.
- Mount YAML settings at `/etc/anway/conf.d`.
- `docker kill -s USR1 anway` writes a backup, and `docker kill -s HUP anway` reloads settings.
- Set the stop timeout above `SHUTDOWN_TIMEOUT`, which is 30 seconds, so that running jobs can report. In Kubernetes, use `terminationGracePeriodSeconds`. A restart does not stop the workers: they reconnect and fetch again.
- To serve the web UI API, set `WEB_ADDR=0.0.0.0:7420` and `WEB_TOKEN`, and publish port 7420 only on a private network.
- Run one replica only. Two servers cannot share one data file.
- The image has no shell. Use a TCP probe on port 7419 for health checks.
