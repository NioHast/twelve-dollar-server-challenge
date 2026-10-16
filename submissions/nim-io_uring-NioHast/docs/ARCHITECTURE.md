# Architecture

This document explains how the submission works, in enough depth to read the
code with understanding. See the [README](../README.md) for the quick start and
[DEVELOPMENT.md](DEVELOPMENT.md) for how it was built.

## Shape of the system

One process. One thread. One event loop. HTTP/1.1 is served directly on
`:80`/`:3000` (no reverse proxy) and every read and write goes to **SQLite**
linked in-process.

```
        io_uring CQEs (default)            epoll_wait (fallback)
               │                                   │
      ┌────────┴────────┐                ┌─────────┴─────────┐
 listen fd         client fd         listen fd          client fd ...
 multishot accept  recv / send       accept4()          recv → parse
 (fixed fds)       (buf pool)        (non-blocking)     │
                   │                                   │
                   └───────────────┬───────────────────┘
                                   ▼
                   http.nim  shared HTTP/1.1 parse + framing
                                   │
                                   ▼
                   api.nim  routing · auth-before-id · exact errors
                     │   │
              jwt.nim│   │jsonbuf.nim  byte-level JSON writer
                     ▼   ▼
                   db.nim ── sqlite3.nim (raw C API) ── vendored SQLite (WAL)
                            │
                            ▼
                   one write per response (backpressure / registered-fd send)
```

## Why one thread

The benchmark box has one vCPU. Extra threads add context switches, cache
pressure and latency jitter without adding throughput, so the whole server is a
single thread driving a single event loop - an io_uring ring by default, an epoll
instance when falling back. This also removes all shared state between request
handlers, which is what lets the rest of the code stay simple: no locks, no
per-request object graph, no worker hand-off.

## Modules

| File | Responsibility |
|---|---|
| `src/server_iouring.nim` | the DEFAULT `io_uring` transport: multishot accept/recv, provided-buffer pool, registered fds, group commit, run-time epoll fallback |
| `src/server_epoll.nim` | the always-built FALLBACK epoll transport: accept, read, write, keep-alive, timeouts, LRU eviction, EMFILE backoff |
| `src/http.nim` | shared, transport-agnostic HTTP/1.1 request parsing + response framing (used by both transports) |
| `src/engine.nim` | shared scaffold used by both transports: `raiseNofile`, listening-socket setup, and the `processRequests` loop |
| `src/iouring.nim` | self-contained raw `io_uring` bindings (no liburing) |
| `src/api.nim` | routing + the five endpoints + exact error semantics; transport-agnostic |
| `src/db.nim` | all SQL; prepared statements; pragmas; read/write lifecycle |
| `src/sqlite3.nim` | minimal raw `{.importc.}` bindings to the vendored SQLite C API |
| `src/jwt.nim` | base64url + HS256 (nimcrypto), constant-time compare |
| `src/jsonbuf.nim` | byte-level JSON writing and minimal request-body parsing |
| `src/model.nim` | the shared `Post` value type |
| `src/config.nim` | environment-only configuration |

The transports (`server_iouring.nim` / `server_epoll.nim`) are the only part that knows
about sockets, and they share the HTTP parsing and response framing in `http.nim`.
They also share the fd-limit bump, the listening-socket setup and the
`processRequests` request/response loop through `engine.nim`, so the two copies
cannot drift. `api.nim` takes plain strings in and returns a status + body, so
both transports and the unit tests share one implementation of the API.

## Request lifecycle

1. **accept**: on epoll, `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` in a loop until
   `EAGAIN`; a freshly accepted fd is registered `EPOLLIN|EPOLLET` and read
   immediately so the accept edge is not lost. On io_uring the same step is a
   multishot accept that installs each socket directly into the fixed-file table.
   Either way the lifecycle below is identical.
2. **read**: edge-triggered discipline: `recv` until `EAGAIN`. Bytes are
   appended to the connection's input buffer. If unparsed input exceeds
   `MaxRequest`, the connection is served-then-closed (see *Bounds*).
3. **parse**: the buffer is walked with a cursor (no per-request front delete).
   Headers, method, path, `Authorization`, `Content-Length` and `Connection` are
   extracted in place. Malformed / non-conforming framing → `400` + close.
4. **route + auth**: `api.handleRequest`. On authenticated routes the bearer
   token is verified **before** the id is parsed, so `POST /posts/abc/like`
   without a token is `401`, not `400`.
5. **SQLite**: one prepared statement per query, executed on the single
   connection. Single-row readers reset the statement immediately so the read
   transaction does not linger; `feed` steps to `SQLITE_DONE`.
6. **respond**: the response is appended to the output buffer and an immediate
   flush is attempted. If it cannot be flushed in full, `EPOLLOUT` is armed.
7. **keep-alive**: unless `Connection: close`, the connection stays open; idle
   timeouts and LRU eviction manage its lifetime.

## Event-loop invariants

These are the rules that make a single-threaded edge-triggered loop correct:

- **Drain to `EAGAIN`.** Under edge-triggered epoll a missed read never re-arms,
  so reads loop until `EAGAIN`.
- **Arm `EPOLLOUT` only after an immediate flush hits `EAGAIN`.** Arming it when
  the socket is already writable would miss the "became writable" edge. Disarm
  when the output buffer drains.
- **Remove the fd from epoll before `close`**, and guard double-close with an
  `active` flag.
- **Never block.** Every syscall on a connection is non-blocking; `EAGAIN` is
  never spun on.
- **Bounded work per event.** Input is capped, so a single connection cannot
  monopolise the loop.

## Connection limits (fd, conntrack) and how they are removed

The naive wall is `RLIMIT_NOFILE = 65535` (per process) plus the kernel
conntrack table (`nf_conntrack_max ≈ 65536`, machine-wide). The server removes
both:

- **Idle keep-alive eviction.** Past a high-water mark, the least-recently-used
  *idle* connections are closed down to a low-water mark (hysteresis). A
  connection with buffered input, pending output or in-flight work is never
  evicted.
- **RST close** (`SO_LINGER {1,0}`) for evicted sockets: the conntrack entry is
  removed immediately instead of lingering in TIME_WAIT, so the churn ceiling is
  far above the static conntrack cap.
- **Large listen backlog** (4096) for reconnect bursts.
- **EMFILE backoff**: on `accept` failure the loop first tries to evict idle
  sockets, then pauses the listener; it never accept-spins.
- **io_uring registered fds (default)**: sockets in the fixed-file table do not
  count against `RLIMIT_NOFILE` at all.
- **Provided-buffer receive pool (default)**: multishot recv draws from a shared,
  lazily-faulted pool, so idle connections hold no recv buffer.

## Transports

| Transport | Select | Notes |
|---|---|---|
| io_uring (default) | n/a (or `TRANSPORT=io_uring`) | multishot accept/recv, provided-buffer pool, registered fds; ABI structs compile-time asserted |
| epoll (always built) | `TRANSPORT=epoll`, or automatic fallback | single thread, edge-triggered; the fallback path |

Both transports feed the same `http.nim`. `start.sh` prefers `bin/server_iouring`
and falls back to `bin/server` when it is missing or dies before serving; the
io_uring binary also re-execs `bin/server` if `io_uring_setup` fails at run time.

## Why this design

**Why two transports.** io_uring wins on CPU and latency, but it can be
unavailable at run time even on an identical OS image: `seccomp` in a container
can block the `io_uring_*` syscalls, the host can set `kernel.io_uring_disabled`,
or a kernel/LSM/VM may refuse the ring. epoll is always present and battle-tested.
Keeping it as an automatic fallback means the server always starts and serves:
best performance by default, guaranteed availability as defence in depth.

**Why io_uring is the default.** It measured ~0.63x epoll's CPU at 10k users
(~0.55x at 20k) with better p50/p95/p99, and it works on the scoring box (Ubuntu
24.04, plain systemd, kernel 6.x). Defaulting to it is safe because of a dual
fallback: build time (a failed io_uring build ships the epoll-only `bin/server`)
and run time (a failed `io_uring_setup` re-execs `bin/server`).

## SQLite integration

- Vendored amalgamation, compiled in-tree (`vendor/sqlite3.c`), so there is no
  system `libsqlite3` dependency.
- One connection, `SQLITE_OPEN_READWRITE|NOMUTEX`, statements prepared once with
  `SQLITE_PREPARE_PERSISTENT`.
- Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`,
  `foreign_keys=ON`, `temp_store=MEMORY`, `cache_size=-8000`,
  `mmap_size=1 GiB` (`SQLITE_MMAP_SIZE`), `wal_autocheckpoint=0`,
  `locking_mode=EXCLUSIVE`.
- WAL checkpointing is explicit and off the request path: autocheckpoint is off,
  and a PASSIVE checkpoint runs at most once a second from the loop after a write.
- Durability: a write is committed before its response is sent; `NORMAL` survives
  a process crash and never corrupts the file.

## Robustness and bounds

See the "Robustness & bounds" section of the [README](../README.md). In short:
no request can abort the process (all handler exceptions, including `Defect`s,
become `500`), framing is strict, input and output are bounded, and parsing is
O(n).
