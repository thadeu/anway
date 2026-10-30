# TLS

TLS is optional. Use it when clients connect over a shared network. A password does not encrypt job data.

If the apps and the server are on the same private network, such as WireGuard, Tailscale, or a VPC, TLS on the server is possibly not necessary.

## Let's Encrypt

The server reads standard PEM files, so a Let's Encrypt certificate works directly.

```bash
certbot certonly --standalone -d jobs.example.com

BIND_ADDR=0.0.0.0:7419 \
SERVER_PASSWORD=change-me \
TLS_CERT_FILE=/etc/letsencrypt/live/jobs.example.com/fullchain.pem \
TLS_KEY_FILE=/etc/letsencrypt/live/jobs.example.com/privkey.pem \
./bin/server
```

Clients use the `tcp+tls` scheme and the domain name, not the IP address:

```text
tcp+tls://:change-me@jobs.example.com:7419
```

### Requirements

- The domain must be public.
- With `--standalone`, port 80 must be available during issue and renewal.
- The server user must have read access to `privkey.pem`. This file usually has root-only access.

### Renewal

The server reads the certificate only at start, so a new certificate needs a restart. Use a certbot deploy hook:

```bash
certbot renew --deploy-hook "systemctl restart job-server"
```

`job-server` is an example service name. Jobs stay on disk during the restart, and clients must reconnect.

## Caddy

Standard Caddy proxies HTTP, and the work protocol is raw TCP. Caddy needs the `caddy-l4` plugin, which requires a custom build. Caddy then ends TLS, renews the certificate, and forwards traffic to `127.0.0.1:7419`.

This setup is not tested. Use it only if the host already runs Caddy.

## Test status

The test suite covers TLS with the Go client only. Test `tcp+tls` with the Ruby and Node clients before you use them in production.
