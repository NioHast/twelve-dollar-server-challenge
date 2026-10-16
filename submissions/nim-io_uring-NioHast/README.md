# Nim: direct HTTP/1.1 on SQLite (the $12 Server Challenge)

A small Twitter-style API (feed, posts, likes) implemented in **Nim 2.2.12**, built from source,
served **directly** on `:80` with a hand-written, single-threaded **io_uring** event loop (the
default) and an automatic **epoll** fallback, over an in-process **SQLite** database.

```bash
sudo bash install.sh                     # root: pinned Nim toolchain -> /opt/nim
bash build.sh                            # user: compile bin/server_iouring + bin/server
SQLITE_PATH=/path/feed.db JWT_SECRET=... HOST=0.0.0.0 PORT=80 bash start.sh
```

Environment: `SQLITE_PATH`, `JWT_SECRET`, `HOST`, `PORT` (defaults `127.0.0.1:3000`). The server is
healthy well within the 60 s the rules allow.

## Documentation

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**: the event loop, modules, request lifecycle, invariants and connection limits.
- **[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)**: how the submission was built, step by step, with the decisions and the robustness review.
- **[docs/PERFORMANCE.md](docs/PERFORMANCE.md)**: what was measured, how to reproduce it, and the honest limitations.
- **[docs/DEPENDENCIES.md](docs/DEPENDENCIES.md)**: every runtime and build dependency, where it comes from, and how it is pinned.

## Transports

**io_uring is the default transport; epoll is the always-built fallback.** Both share the same HTTP
parsing and response framing (`src/http.nim`), so they emit byte-identical responses.

| Transport | How to select | What it adds |
|---|---|---|
| **io_uring** (default) | nothing, or `TRANSPORT=io_uring` | one thread, completion-queue driven; provided-buffer receive pool; registered file descriptors (fds no longer count against `RLIMIT_NOFILE`) |
| **epoll** (always available) | `TRANSPORT=epoll`, or automatic fallback | one thread, edge-triggered epoll; idle keep-alive eviction + RST close |

```bash
bash build.sh                            # builds bin/server_iouring (default) AND bin/server
TRANSPORT=epoll bash start.sh            # force the epoll transport
```

`start.sh` runs `bin/server_iouring` by default. If that binary is missing, is not executable, or
exits before it can serve, `start.sh` falls back to `bin/server`. The io_uring binary additionally
re-execs `bin/server` itself when `io_uring_setup` fails at run time (seccomp,
`kernel.io_uring_disabled`, no io_uring in the kernel), so a host that blocks io_uring never leaves the
service down. See [Why this design](#why-this-design).

## Why this design

Why this submission ships two transports, and why io_uring is the default.

### Why two transports (io_uring + epoll)

io_uring wins on CPU and latency, but it is **not guaranteed to exist at run time - even on an
identical OS image**. `seccomp` in a container can block the `io_uring_*` syscalls, the host can set
`kernel.io_uring_disabled`, an LSM or an older kernel may refuse the ring, and a VM may lack it
entirely. epoll, by contrast, is always present and battle-tested. Keeping epoll as an automatic
fallback means the service **always starts and serves**: best performance by default, guaranteed
availability as defence in depth.

### Why io_uring is the default

It measured **~0.63x epoll's CPU at 10,000 users** (and **~0.55x at 20,000**), with better p50/p95/p99,
and it works on the scoring box (Ubuntu 24.04, plain systemd, kernel 6.x). Defaulting to it is safe
because of a **dual fallback**:

- **build time** - if the io_uring build fails for any reason, `build.sh` warns and ships the
  epoll-only `bin/server`, still exiting 0;
- **run time** - if `io_uring_setup` itself fails, the process re-execs the sibling `bin/server`, so a
  blocked io_uring never leaves the service down.

Per-feature failures short of that (a rejected setup flag, no multishot recv, no
multishot/direct accept, no buf ring) degrade in place without a restart.

## Toggles

Every switch, with its default. When nothing is set, the safe defaults are used.

Build time (`build.sh`):

| Variable | Default | Effect |
|---|---|---|
| `PGO` | `1` (on) | `PGO=0` builds without PGO: faster and deterministic, but ~16-20% more CPU per request |
| `PGO_TRAIN_ITERS` | `60` | PGO training iterations (larger = more representative profile, slower build) |
| `IO_URING` | `1` (on) | `IO_URING=0` skips the io_uring build, leaving the epoll-only `bin/server` |

Run time (environment for `start.sh`):

| Variable | Default | Effect |
|---|---|---|
| `TRANSPORT` | `io_uring` | `epoll` forces the epoll transport (`bin/server`) |
| `IOURING_RECV` | `multishot` | `oneshot` re-arms a one-shot recv (the p99 fallback) |
| `IOURING_ACCEPT` | `multishot` | `oneshot` re-arms a one-shot accept |
| `IOURING_ACCEPT_DIRECT` | `1` (on) | `0` forces the register-per-accept path instead of direct fixed-slot accept |
| `IOURING_BUFS` | `2048` | provided-buffer pool size; rounded down to a power of two, clamped to `[64, 8192]` |
| `IOURING_BUFRING` | `1` (on) | `0` forces the `IORING_OP_PROVIDE_BUFFERS` list instead of the registered buf ring |
| `GROUP_COMMIT` | `1` (on) | `0` disables write group commit (per-request autocommit) |
| `EVICT_HIGH` | `63000` | High-water mark that triggers idle keep-alive eviction |
| `EVICT_LOW` | `56000` | Low-water mark eviction drains down to (hysteresis) |
| `TCP_DEFER_ACCEPT_SEC` | `1` | Seconds the kernel waits for the first client byte before accepting; `0` disables |
| `SQLITE_MMAP_SIZE` | `1073741824` | SQLite `mmap_size` in bytes (1 GiB); `0` disables mmap |
| `EPOLL_PARK_US` | `0` (off) | epoll only: park this many µs before the next wait when a pass carried <32 events, to batch arrivals (trades a little latency for fewer wake-ups) |
| `TCP_NOTSENT_LOWAT_BYTES` | `0` (off) | Cap the unsent write queue per connection (e.g. `131072`); on io_uring this forces the register-per-accept path |
| `TCP_QUICKACK` | `0` (off) | `1` sets `TCP_QUICKACK` on accepted sockets; on io_uring this forces the register-per-accept path |

Configuration: `SQLITE_PATH`, `JWT_SECRET`, `HOST`, `PORT` (default `127.0.0.1:3000`).

Everything else is deliberately fixed in code and not tunable at run time: one thread per process,
persistent prepared statements, WAL + `synchronous=NORMAL`, idle eviction + RST, the 4096 listen
backlog, EMFILE backoff, the O(n) parser, and off-path WAL checkpoints.

## Architecture

One process, **one thread**, one event loop - io_uring by default, epoll as the fallback. On a 1 vCPU
box more threads only add context switches; one loop keeps latency predictable and memory per
connection tiny.

```
      io_uring completion queue          epoll_wait (edge-triggered)   ← default / fallback
               │                                   │
      ┌────────┴────────┐                ┌─────────┴─────────┐
 listen fd         client fd         listen fd          client fd ...
 multishot accept  recv / send       accept4()          recv → parse
 (registered fds)  (buf pool)        (nonblock)         │
                   │                                   │
                   └───────────────┬───────────────────┘
                                   ▼
              http.nim  shared HTTP/1.1 parse + response framing
                                   │
                                   ▼
              api.nim   routing · auth-before-id · exact errors
                │   │
         jwt.nim│   │jsonbuf.nim  byte-level JSON writer
                ▼   ▼
              db.nim ── sqlite3.nim (raw C API) ── in-process SQLite (WAL)
                      │
                      ▼
              one write per response  (backpressure / registered-fd send)
```

| File | Responsibility |
|---|---|
| `src/server_iouring.nim` | **default** io_uring loop: multishot accept/recv, provided-buffer pool, registered fds, group commit, run-time epoll fallback |
| `src/server_epoll.nim` | **always-built fallback** epoll loop, accept/read/write, keep-alive, timeouts, backpressure, **eviction** |
| `src/http.nim` | shared, transport-agnostic HTTP/1.1 request parsing + response framing (used by both transports) |
| `src/engine.nim` | shared scaffold used by both transports: `raiseNofile`, listening-socket setup, and the `processRequests` loop |
| `src/iouring.nim` | self-contained raw io_uring bindings (no liburing) |
| `src/api.nim` | routing + the five endpoints + exact error semantics (transport-agnostic) |
| `src/db.nim` | all SQL; prepared statements; pragmas; durability ordering |
| `src/sqlite3.nim` | minimal raw `{.importc.}` bindings to the vendored SQLite C API |
| `src/jwt.nim` | base64url + HS256 HMAC verification (nimcrypto), constant-time compare |
| `src/jsonbuf.nim` | byte-level JSON writing and minimal request-body parsing |
| `src/model.nim`, `src/config.nim` | shared types; env-only configuration |

**Request flow:** the event source (io_uring CQEs or `epoll_wait`) signals a connection → drain the
socket → `http.nim` parses every complete pipelined request in place → `api.handleRequest` (auth is
checked *before* any id parsing; every read hits SQLite) → `http.nim` frames the response bytes into
the connection's output buffer → flush until the socket would block; anything left is drained on the
next readiness event so a slow reader can never stall the loop.

### Event-loop invariants
- **Drain to EAGAIN.** Under edge-triggered epoll a missed read never re-arms, so reads loop until
  `EAGAIN`; a freshly accepted fd is drained immediately to close the ET accept race.
- **Never block.** `recv`/`send`/`accept` are non-blocking; `EAGAIN` is never a busy loop.
- **Backpressure.** Unflushed output stays buffered with `EPOLLOUT` armed; it is only armed after an
  immediate flush returns `EAGAIN`, so an already-writable socket is never missed.
- **Resource guards.** 16 KiB max header block; 1 MiB max unparsed input; idle 75 s / stalled 30 s.
- **Sparse fd-indexed state.** Per-connection state is a small pooled `seq[Conn]` slot.

## Performance optimizations

**Transport and architecture**
- **No reverse proxy.** Nginx costs 14-20% of the single core; serving HTTP directly avoids that overhead.
- **io_uring by default, epoll as fallback.** Both share `src/http.nim`; io_uring measured ~0.63x
  epoll's CPU at 10k users with better tail latency.
- **One event loop, one thread.** No worker pool, no thread handoff, no per-request object graph.
- **One write per response**, with backpressure (`EPOLLOUT` on epoll, send requeue on io_uring) so a
  slow reader never stalls the loop.

**Data path**
- **One SQLite connection, statements prepared once.** Statements use `SQLITE_PREPARE_PERSISTENT`; SQL
  never re-parses, and `SQLITE_ENABLE_QPSG` keeps the plan stable if a statement is ever re-prepared.
- **JSON serialized in Nim**, not by SQLite's `json_object`/`json_group_array`: the response bytes are
  written by a small escaping routine, avoiding extra SQLite-side allocation and function calls.
- **mmap + a small page cache.** Reads come from the OS page cache (size via `SQLITE_MMAP_SIZE`,
  default 1 GiB); the SQLite cache stays small.
- **WAL checkpoint off the hot path.** Autocheckpoint is disabled (`wal_autocheckpoint=0`) and a
  **PASSIVE** checkpoint runs at most once a second from the idle branch of the loop, so no request
  pays a checkpoint fsync; `journal_size_limit` still bounds the `-wal` file.
- **Write group commit** (default on): writes issued during one event-loop pass share a single
  transaction that commits before any dependent response is sent (`synchronous=NORMAL`, the documented
  floor the rules allow).
- **O(n) request parsing**: the input buffer is walked with a cursor and compacted once per event, so a
  large pipelined buffer is parsed in O(n) rather than O(n^2).

**Build**
- **Profile-guided optimization (PGO)**: a two-pass build trained on a short synthetic workload, with a
  plain-build fallback when PGO is unavailable.
- **Native + LTO + SQLite trims**: `-O3 -march=native -fno-semantic-interposition -fno-plt
  --stacktrace:off` and `-d:lto`, with SQLite compiled `THREADSAFE=0` and trimmed (`USE_ALLOCA=1`,
  `LIKE_DOESNT_MATCH_BLOBS`, `OMIT_DECLTYPE`, `OMIT_AUTOINIT`, `OMIT_*`, `DQS=0`, JSON1). See [Build](#build).

**Capacity**
- **Idle eviction + RST, large listen backlog, TCP_DEFER_ACCEPT, EMFILE backoff, io_uring registered fds**
  (see [Peak connections](#peak-connections-the-real-limit)).

## Peak connections (the real limit)

`RLIMIT_NOFILE` on the benchmark box is `65535` and cannot be raised from inside the process, so the
server removes that wall itself:

- **Idle keep-alive eviction (default).** Past a high-water mark the least-recently-used *idle*
  connections are closed (never one with a request in flight or unflushed output), with hysteresis.
- **RST close** (`SO_LINGER {1,0}`) for evicted sockets, avoids TIME_WAIT and keeps conntrack entries
  short-lived.
- **Large listen backlog** (4096) so reconnect bursts do not drop SYNs.
- **EMFILE backoff.** On `accept()` failure the loop evicts idle sockets first, then pauses the
  listener; it never spins.
- **io_uring registered fds (default).** Registered sockets do not consume process fds at all
  (verified: 800 held connections, 0 extra process fds).
- **Provided-buffer receive pool (default).** Multishot recv draws from a shared, lazily-faulted
  pool, so idle connections hold no recv buffer.

This is a general capacity policy (what Nginx does when it runs out of connections), not benchmark
detection.

## Robustness & bounds

The server is built so that **no request can crash it** and every per-event operation is bounded:

- **No request aborts the process.** The request handler is wrapped so that any exception, including
  `Defect`s such as integer-overflow from hostile input (e.g. `GET /posts/99999999999999999999999`),
  is answered with `500` instead of propagating. Request ids are range-checked *before* the multiply,
  so over-long ids return `400`. (An earlier revision could be killed by one crafted id; this is fixed.)
- **Strict framing.** `Content-Length` must be all digits (no sign, no empty, no conflicting duplicate);
  `Transfer-Encoding` is rejected. Bad framing → `400` + close, never a body re-parsed as a request.
- **Bounded input.** Buffered *unparsed* input per connection is capped (128 KiB), and every complete
  request in the buffer is processed each event. A client that pipelines more than the cap in a single
  burst is served up to the cap and then disconnected. Real clients, including the benchmark's
  `load.js`, send one request at a time, so this bound is never reached in the scored workload.
- **Bounded output / backpressure.** A response that cannot be flushed in full stays buffered and
  `EPOLLOUT` is armed; a slow reader can never stall the loop or grow memory without bound.
- **Timeouts.** Idle keep-alive 75 s; stalled mid-request 30 s.
- **Linear parsing.** Each event compacts the consumed input prefix once (no per-request front-delete),
  so a large pipelined buffer parses in O(n) rather than O(n²).

## Durability

`WAL` + `synchronous=NORMAL`. A `201` means the row is committed before the response is written.
`NORMAL` survives a process crash; on power loss the newest committed transactions may be lost while
the database stays uncorrupted. `synchronous=OFF` (which can corrupt the file) is not used.

## Build

- **Toolchain:** pinned Nim `2.2.12` binary tarball (sha256-verified in `install.sh`).
- **Two binaries:** `build.sh` builds `bin/server_iouring` (the default transport) and `bin/server`
  (the epoll fallback). The io_uring build is best-effort: on any failure the script warns, drops the
  half-written binary, and still exits 0 with the epoll-only server. `IO_URING=0` skips it.
- **SQLite:** the `3.54.0` amalgamation is downloaded by `build.sh` (sha256-verified, `curl --retry 3`)
  and compiled in-tree, so the submission has **no system `libsqlite3` dependency** and builds cleanly
  from source.
- **nimcrypto:** `src/jwt.nim` needs it for HMAC-SHA256. It is vendored the same way (pinned `0.7.3`
  tarball, sha256-verified, extracted into `vendor/nimcrypto`) and `config.nims` adds its path to every
  compile, so the build never depends on anything installed in a user Nimble directory.
- **Nim flags:** `--threads:off --mm:arc -d:release -d:lto --stacktrace:off`, plus
  `--passC:"-O3 -march=native -fno-semantic-interposition -fno-plt"` and `--passL:"-Wl,-O1"`.
  Runtime checks are intentionally **kept on** (`-d:release`, never `-d:danger`): a crash during the
  benchmark must never happen, and the check cost is negligible next to the syscall and SQL work.
- **SQLite trims:** `-DSQLITE_THREADSAFE=0` (the server is single-threaded), `-DSQLITE_DQS=0`,
  `-DSQLITE_DEFAULT_MEMSTATUS=0`, `-DSQLITE_USE_ALLOCA=1`, `-DSQLITE_LIKE_DOESNT_MATCH_BLOBS`,
  `-DSQLITE_OMIT_DECLTYPE`, `-DSQLITE_OMIT_AUTOINIT` (with one `sqlite3_initialize()` in `openDb`),
  `-DSQLITE_OMIT_DEPRECATED`, `-DSQLITE_OMIT_LOAD_EXTENSION`, `-DSQLITE_OMIT_PROGRESS_CALLBACK`,
  `-DSQLITE_OMIT_SHARED_CACHE`, `-DSQLITE_MAX_EXPR_DEPTH=0`, `-DSQLITE_TEMP_STORE=3`, `-DSQLITE_ENABLE_JSON1`,
  `-DSQLITE_ENABLE_QPSG`.

## Testing

- `bash test/run.sh submissions/nim-io_uring-NioHast`: the official 42-check suite, byte-exact; with no
  environment set it runs the default io_uring transport.
- `TRANSPORT=epoll bash test/run.sh …`: the epoll fallback (also 42/42).
- Unit tests under `tests/`: `test_db.nim` (queries vs the seed DB and the `sqlite3` CLI),
  `test_jwt.nim` (HS256 against `openssl` vectors), `test_iouring.nim` (io_uring ABI + SQE/CQE round-trip).

## Development notes

Before writing the event loop the author **reverse-engineered `mummy`**, a fast,
multi-threaded Nim HTTP/1.1 server, to study how it wires epoll, per-connection
buffers and keep-alive, then implemented this single-threaded server from scratch.
No `mummy` code is used, imported, or shipped.

## Compliance (challenge rules)

- **SQLite only**: every read/write is in-process SQLite (rule 2); no in-memory store, no external
  cache.
- **No caching across requests** (rule 5), no response cache; JSON and like counts are computed per
  request. Prepared statements and the SQLite page cache are allowed and used.
- **Durable before responding** (rule 6), commit before the response; no `synchronous=OFF`.
- **No hard-coded responses / no benchmark detection** (rule 7), everything derives from SQLite.
- **Nginx optional / direct** (rule 9), direct bind.
- **Built from source, pinned** (rule 10), pinned toolchain and pinned SQLite; nothing prebuilt is
  shipped; no system library beyond a C toolchain and libc is required.
- **No kernel tuning, no pinning** (rule 11), only in-process settings (socket options, SQLite
  pragmas, event loop, `setrlimit` of our own soft limit).

## Repository layout

```
submissions/nim-io_uring-NioHast/
  install.sh  build.sh  start.sh  README.md  .gitignore  config.nims
  docs/   ARCHITECTURE.md  DEVELOPMENT.md  PERFORMANCE.md  DEPENDENCIES.md
  src/    server_iouring.nim server_epoll.nim engine.nim iouring.nim http.nim
          api.nim db.nim sqlite3.nim jwt.nim jsonbuf.nim model.nim config.nim
  tests/  test_db.nim test_jwt.nim test_iouring.nim
  vendor/ (downloaded by build.sh; gitignored)   bin/ nimcache/ (gitignored)
```

## License

MIT, under the repository [license](../../LICENSE).
