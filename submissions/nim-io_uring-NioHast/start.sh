#!/usr/bin/env bash
# Starts the server in the foreground. Configuration is environment-only.
#
# Default transport is the io_uring server (bin/server_iouring), which beats epoll
# on CPU. TRANSPORT=epoll forces the epoll server (bin/server). TRANSPORT=io_uring
# or unset prefers io_uring and falls back to bin/server when the io_uring binary
# was not built, is not executable, or exits before it can serve. The io_uring
# binary also re-execs bin/server itself on an unrecoverable io_uring_setup
# failure, so a blocked io_uring never leaves the service down.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
TRANSPORT="${TRANSPORT:-io_uring}"

if [ "$TRANSPORT" = "io_uring" ] && [ -x "$DIR/bin/server_iouring" ]; then
  "$DIR/bin/server_iouring" &
  iouring=$!
  # A server that has already exited never came up (loader/exec/setup failure);
  # one still alive after this window owns the service. We deliberately do NOT
  # fall back after a later non-zero exit: a normal shutdown signal also exits
  # non-zero, and respawning epoll then would leave a second server behind.
  sleep 0.5
  if kill -0 "$iouring" 2>/dev/null; then
    trap 'kill -TERM "$iouring" 2>/dev/null || true' TERM INT
    rc=0; wait "$iouring" || rc=$?
    exit "$rc"
  fi
  rc=0; wait "$iouring" 2>/dev/null || rc=$?
  echo "start.sh: bin/server_iouring exited before serving (rc=$rc); falling back to bin/server" >&2
fi

exec "$DIR/bin/server"
