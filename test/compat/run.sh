#!/usr/bin/env bash
# Runs a ring of producers and consumers across all client languages.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

work="$(mktemp -d)"
addr="127.0.0.1:17420"
password="${1:-}"

BIND_ADDR="$addr" DATA_DIR="$work/data" SERVER_PASSWORD="$password" SHUTDOWN_TIMEOUT=4s "$root/bin/server" 2>"$work/log" &
server=$!
trap 'kill "$server" 2>/dev/null || true; rm -rf "$work"' EXIT
sleep 1

if [ -n "$password" ]; then
  export FAKTORY_URL="tcp://:$password@$addr"
else
  export FAKTORY_URL="tcp://$addr"
fi

go_client() { (cd test/compat/go && go run . "$@"); }
ruby_client() { (cd test/compat/ruby && bundle exec ruby compat.rb "$@"); }
node_client() { (cd test/compat/node && node compat.mjs "$@"); }
bun_client() { (cd test/compat/node && bun compat.mjs "$@"); }

clients=(go ruby node bun)
beats=()

for i in "${!clients[@]}"; do
  producer="${clients[$i]}"
  consumer="${clients[$(((i + 1) % ${#clients[@]}))]}"
  jid="compat-$producer-$consumer-$RANDOM"

  "${producer}_client" push "$jid"
  "${consumer}_client" consume "$jid"
done

for client in "${clients[@]}"; do
  jid="compat-fail-$client-$RANDOM"

  go_client push "$jid"
  "${client}_client" fail "$jid"
done

retries="$(go_client stats 0)"

if [ "$retries" != "${#clients[@]}" ]; then
  echo "expected ${#clients[@]} retries, got $retries"
  exit 1
fi

echo "FAIL accepted by the server: $retries retries"

go_client admin 0
go_client batch 0

for client in "${clients[@]}"; do
  "${client}_client" beat 0 >"$work/beat-$client" 2>&1 &
  beats+=("$!")
done

sleep 2
kill -TERM "$server"

for pid in "${beats[@]}"; do
  wait "$pid"
done

for client in "${clients[@]}"; do
  grep -q "beat quiet" "$work/beat-$client"
  grep -q "beat terminate" "$work/beat-$client"
  cat "$work/beat-$client"
done

wait "$server"
echo "compat passed"
