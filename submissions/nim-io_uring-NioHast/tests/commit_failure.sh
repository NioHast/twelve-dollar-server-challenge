#!/usr/bin/env bash
# commit_failure.sh - a write-lock failure on the write path.
#
# A second SQLite connection holds the WAL write lock (BEGIN IMMEDIATE). The
# server's group-commit pass then tries its own BEGIN IMMEDIATE and must wait
# the 5 s busy_timeout, fail the batch, and answer the POST with 500 - never
# 201, and never persist the row. Because the batch never opens, no success
# response is produced for this request.
#
# NOT covered here: the separate adversarial path where a 201 has already been
# buffered and a later COMMIT fails, which flushDeferredSlots rewrites to 500
# (src/server.nim). A COMMIT-time SQLITE_BUSY is not constructible in this
# server: it never runs RESTART/FULL checkpoints, so the write-lock acquisition
# below is the reachable SQLITE_BUSY failure on the write path. The rewrite is
# verified by inspection and recorded as an untested adversarial path.
#
# The lock is held for 8 s (longer than busy_timeout but well under the ~15 s
# test budget), then released; the test also proves the server is healthy and
# serving ordinary requests afterwards.
#
#   bash tests/commit_failure.sh            # ~10 s
#   PORT=3484 bash tests/commit_failure.sh
#
# Needs: curl, python3, openssl. Never uses `pkill -f`; cleanup is `fuser` +
# pgrep -P.
set -uo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"          # submissions/nim-io_uring-NioHast
ROOT="$(cd "$DIR/../.." && pwd)"                  # repo root
SEED="$ROOT/seed/feed.db"
BIN="$DIR/bin/server"
PORT="${PORT:-3484}"
HOST="${HOST:-127.0.0.1}"
JWT_SECRET="${JWT_SECRET:-twelve-dollar-challenge}"
USER_ID=1; USER_NAME="golden_ember_1"

for t in curl python3 openssl; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing required tool: $t" >&2; exit 2; }
done
[ -x "$BIN" ]  || { echo "missing binary: $BIN (run: bash build.sh)" >&2; exit 2; }
[ -f "$SEED" ] || { echo "missing seed: $SEED (run: bash seed/make-seed.sh)" >&2; exit 2; }

TMP="$(mktemp -d)"
SQLITE_PATH="$TMP/feed.db"
SRV_PID=""
HOLDER_PID=""

cleanup() {
  [ -n "$HOLDER_PID" ] && kill "$HOLDER_PID" 2>/dev/null || true
  if [ -n "$SRV_PID" ]; then
    for c in $(pgrep -P "$SRV_PID" 2>/dev/null || true); do kill "$c" 2>/dev/null || true; done
    kill "$SRV_PID" 2>/dev/null || true
  fi
  fuser -k -TERM "${PORT}/tcp" >/dev/null 2>&1 || true
  sleep 0.2
  fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

pass=0; fail=0
ok()  { echo "  ok    $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; [ -n "${2:-}" ] && echo "        $2"; fail=$((fail+1)); }

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
sign() {
  local h p s
  h="$(printf '%s' '{"alg":"HS256","typ":"JWT"}' | b64url)"
  p="$(printf '%s' "$1" | b64url)"
  s="$(printf '%s' "$h.$p" | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary | b64url)"
  echo "$h.$p.$s"
}
now=$(date +%s)
TOKEN="$(sign "{\"sub\":\"$USER_ID\",\"username\":\"$USER_NAME\",\"iat\":$now,\"exp\":$((now+3600))}")"
PROBE="commit-failure-probe-$$-$now"

cp "$SEED" "$SQLITE_PATH"
export JWT_SECRET HOST PORT SQLITE_PATH GROUP_COMMIT=1
bash "$DIR/start.sh" >"$TMP/server.log" 2>&1 &
SRV_PID=$!

up=0
for _ in $(seq 100); do
  if curl -sf "http://$HOST:$PORT/health" >/dev/null 2>&1; then up=1; break; fi
  kill -0 "$SRV_PID" 2>/dev/null || break
  sleep 0.1
done
if [ "$up" != 1 ]; then
  echo "COMMIT_FAILURE pass=0 fail=1"
  echo "server did not become healthy; log:" >&2
  cat "$TMP/server.log" >&2
  exit 1
fi

# Hold the one WAL write lock for 8 s. The server's busy_timeout is 5 s.
python3 - "$SQLITE_PATH" "$TMP/locked" <<'PY' &
import sqlite3, sys, time
path, locked = sys.argv[1], sys.argv[2]
con = sqlite3.connect(path, timeout=10, isolation_level=None)
con.execute("PRAGMA busy_timeout=10000")
con.execute("BEGIN IMMEDIATE")
open(locked, "w").write("locked\n")
time.sleep(8)
con.execute("ROLLBACK")
con.close()
PY
HOLDER_PID=$!

held=0
for _ in $(seq 100); do
  [ -f "$TMP/locked" ] && { held=1; break; }
  kill -0 "$HOLDER_PID" 2>/dev/null || break
  sleep 0.05
done
if [ "$held" != 1 ]; then
  echo "COMMIT_FAILURE pass=0 fail=1"
  echo "lock holder never acquired the write lock" >&2
  exit 1
fi

echo "Commit failure: write lock held, POST /posts must fail after busy_timeout"
start_ms=$(date +%s%3N)
status="$(curl -s --max-time 12 -o "$TMP/body" -w '%{http_code}' \
  -X POST "http://$HOST:$PORT/posts" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"body\":\"$PROBE\"}" 2>"$TMP/curl.err" || true)"
end_ms=$(date +%s%3N)
elapsed=$(( end_ms - start_ms ))
BODY="$(cat "$TMP/body" 2>/dev/null || true)"

if [ "$status" = 500 ]; then
  ok "POST /posts -> 500 while the write lock is held (${elapsed} ms)"
else
  bad "POST /posts status" "want 500, got '$status' after ${elapsed} ms (body: $BODY; $(cat "$TMP/curl.err"))"
fi
if [ "$BODY" = '{"error":"internal server error"}' ]; then
  ok "POST body is the exact internal-error JSON"
else
  bad "POST body" "got '$BODY'"
fi

wait "$HOLDER_PID" 2>/dev/null || true
HOLDER_PID=

# The failed write must leave no row behind.
found="$(python3 - "$SQLITE_PATH" "$PROBE" <<'PY'
import sqlite3, sys
path, probe = sys.argv[1], sys.argv[2]
con = sqlite3.connect(path, timeout=10)
n = con.execute("SELECT count(*) FROM posts WHERE body = ?", (probe,)).fetchone()[0]
print(n)
con.close()
PY
)"
if [ "$found" = 0 ]; then
  ok "probe row is absent from SQLite after the failed POST"
else
  bad "probe row persisted" "found $found matching rows"
fi

health="$(curl -s -o "$TMP/health" -w '%{http_code}' --max-time 3 \
          "http://$HOST:$PORT/health" 2>/dev/null || true)"
if [ "$health" = 200 ]; then
  ok "/health -> 200 after the lock is released"
else
  bad "/health" "got '$health'"
fi

# The server must still accept a normal write after the failed one.
now=$(date +%s)
TOKEN="$(sign "{\"sub\":\"$USER_ID\",\"username\":\"$USER_NAME\",\"iat\":$now,\"exp\":$((now+3600))}")"
status="$(curl -s --max-time 5 -o "$TMP/body2" -w '%{http_code}' \
  -X POST "http://$HOST:$PORT/posts" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"body\":\"after-commit-failure\"}" 2>/dev/null || true)"
if [ "$status" = 201 ]; then
  ok "POST /posts -> 201 after the failure (server recovered)"
else
  bad "recovery POST" "want 201, got '$status' (body: $(cat "$TMP/body2" 2>/dev/null))"
fi

if grep -q "dispatch error" "$TMP/server.log"; then
  bad "server logged a dispatch error" "$(grep 'dispatch error' "$TMP/server.log")"
else
  ok "no dispatch errors logged"
fi

echo "COMMIT_FAILURE pass=$pass fail=$fail"
[ "$fail" = 0 ]
