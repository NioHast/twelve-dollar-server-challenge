# How this submission was built (step by step)

A walk-through of the process, decisions and tests behind this submission, so a
reader can follow the reasoning rather than just read the final code.

## 0. Understand the job before writing code

The task is a small Twitter-style API (feed, posts, likes) on **SQLite**, served
**fast** on a **1 vCPU / 2 GB** droplet. The score is the most **users** (k6
virtual users) that pass a 5-minute hold with **p95 < 500 ms**, **p99 < 1 s** and
**< 1 % failed**. So the job is not "write an API"; it is "hold a very large
number of idle keep-alive connections and answer each request cheaply".

Before writing anything I read, carefully:

- `SPEC.md`: the exact endpoints and error strings,
- `test/test.sh`: the 42 checks (byte-exact golden output, auth ordering, edge
  cases), because **the test is the contract**,
- `schema.sql`, `seed/`, `bench/load.js`, the data model and the exact load
  shape (think times, request mix).

Key realisation: the load is **~2.2 requests per ~20.5 s per user**. So the
battle is (a) holding connections cheaply, (b) low per-request CPU, (c) stable
tail latency, not raw request throughput.

## 1. Freeze the interfaces, get correctness first

I split the server into transport-agnostic modules (`api`, `db`, `jwt`,
`jsonbuf`, `model`, `config`) with frozen signatures, so the HTTP layer could be
swapped without touching the API logic.

To validate correctness quickly, I first stood the API up on a ready-made
fast Nim HTTP server (**`mummy`**, which I spent time reverse-engineering to see
how it wires epoll, per-connection buffers and keep-alive), and drove the
official `test/run.sh`. Getting **42/42** on that bootstrap proved the API, JSON,
JWT and SQL were right **before** any performance work. That bootstrap is not
shipped, it was a scaffold, later replaced by the hand-written loop.

## 2. Replace the bootstrap with a raw epoll server

The default transport (`src/server_epoll.nim`) is written from scratch:

- single thread, edge-triggered epoll,
- non-blocking `accept4` / `recv` / `send`,
- in-place HTTP/1.1 parsing with keep-alive and pipelining,
- backpressure via `EPOLLOUT`.

Getting the edge-triggered invariants right (drain-to-`EAGAIN`, arm `EPOLLOUT`
only after a real `EAGAIN`, del-before-close) was the careful part. Again gated
on `test/run.sh` = **42/42**, plus a concurrency self-test (thousands of
keep-alive requests, no truncated responses).

## 3. Remove the two walls: fd limit and conntrack

The biggest ceiling is not CPU but `RLIMIT_NOFILE` (65535) and the kernel
conntrack table (~64k). So the loop got:

- **LRU idle-keep-alive eviction** with hysteresis,
- **RST close** for evicted sockets (removes conntrack entries immediately),
- a **large listen backlog** and **EMFILE backoff**.

This is a general capacity policy, valid even after the connection cap.

## 4. Vendor SQLite and set the build flags

To keep the build resilient, the submission avoids depending on any system
library beyond a C toolchain and libc:

- `build.sh` downloads the **pinned** SQLite amalgamation, verifies its
  **sha256**, and compiles it **in-tree** → no `libsqlite3` dependency,
- SQLite is compiled with size/speed trims (`THREADSAFE=0`, `OMIT_*`, JSON1),
- Nim flags: `-d:release -d:lto` plus `-O3 -march=native`. Runtime checks stay
  **on** (`-d:release`, never `-d:danger`), a crash in the benchmark would be
  worse than any micro-optimisation.

## 5. Transports (io_uring default, epoll fallback)

- **io_uring** (default): multishot accept/recv over a shared provided-buffer
  pool, with registered socket fds that do not count against `RLIMIT_NOFILE`.
  Bindings are hand-written (no liburing); the ABI is compile-time asserted.
  Built by default; if the build fails, `build.sh` ships the epoll-only server.
- **epoll** (always-built fallback): the original edge-triggered loop, used with
  `TRANSPORT=epoll` or automatically when the io_uring binary is missing or
  `io_uring_setup` fails at run time.

Both transports share the HTTP parsing and response framing extracted into
`src/http.nim`, so responses are byte-identical on either path; both transports
pass 42/42.

## 6. Robustness review (why there is a "bounds" section)

After the features were in, I did a deliberate bug hunt: static review of every
module plus adversarial tests (fragmented requests, pipelined requests, slow
readers, half-open sockets, random/malformed input, connection churn, huge
pipelined buffers).

It found a **critical bug**: `parsePositiveId` checked an overflow one step too
late, so `GET /posts/9223372036854775808` raised `OverflowDefect` which the
handler did not catch, so one request **killed the process**. That is exactly the
kind of bug that must never reach a benchmark. Fixes:

- check the id bound **per digit** before multiplying,
- wrap the handler so **any** exception (including `Defect`s) becomes `500`
  instead of aborting the process,
- strict `Content-Length` / reject `Transfer-Encoding`,
- bound buffered input; process all buffered requests per event with no
  stall-prone resume state,
- bound the WAL (a ~1/s PASSIVE checkpoint off the request path),
- release oversized buffers on close,
- replace an O(n²) front-delete parser with an O(n) cursor.

Each fix was re-verified: **42/42**, the crafted id returns **400** with the
server alive, and the adversarial tests still pass.

## 7. Testing strategy

- **Official suite:** `test/run.sh` (42/42), for both epoll and io_uring.
- **Unit tests:** `tests/test_db.nim` (queries vs the seed DB and the `sqlite3`
  CLI), `tests/test_jwt.nim` (HS256 against `openssl` vectors),
  `tests/test_iouring.nim` (io_uring ABI + SQE/CQE round-trip).
- **Adversarial battery:** fragmentation, pipelining, slow readers,
  half-open/stalled sockets, malformed input, churn (RSS/fd stability).
- **Load:** the repo's `bench/load.js` (k6) and a lean custom generator for
  connection-capacity probing (see [PERFORMANCE.md](PERFORMANCE.md)).

## Lessons

- The contract is the test suite, not the prose spec, read it first.
- Get correctness on a scaffold, then replace the scaffold with the real thing.
- Edge-triggered epoll is easy to get subtly wrong; the invariants are everything.
- The hard walls are the fd limit and conntrack, not CPU, design for them.
- Hunt for the one-request-kills-the-server bug **before** the benchmark, not
  during it.

## 8. Post-submission consolidation and tuning

After the submission was sent, the code was reviewed for readability and then
given a set of opt-in performance passes, all still gated on `test/run.sh` = 42/42
on both transports:

- **Consolidation (no behaviour change).** Dead fields/constants were removed, the
  shared transport constants were given one home, and the connection resets went
  from field-by-field writes to a single `Conn(...)` constructor. The socket
  setup, the process fd-limit bump and the `processRequests` loop - previously
  copy-pasted in both transports - moved into a new `src/engine.nim`, so each
  transport keeps only a thin adapter.
- **WAL checkpoint off the hot path.** `wal_autocheckpoint=0` plus a PASSIVE
  checkpoint at most once a second, so no request pays a checkpoint fsync.
- **`SQLITE_ENABLE_QPSG`** so a parameterized statement keeps a stable plan across
  re-preparation.
- **`SQLITE_MMAP_SIZE`** exposes the SQLite mmap size (default 1 GiB).
- **`curl --retry 3`** in `build.sh`, so a transient network blip cannot fail a
  clean build.
- **Opt-in TCP knobs** (default off): `TCP_NOTSENT_LOWAT_BYTES`, `TCP_QUICKACK`
  (both fall back to register-per-accept on io_uring, since a directly-accepted
  socket has no userspace fd), and `EPOLL_PARK_US` adaptive batching on epoll.

Candidates that were evaluated and **not** adopted, with reasons: a separate
read/write SQLite connection (complexity vs. a single-writer workload),
`MADV_HUGEPAGE` on the SQLite mmap (the mmap is SQLite-managed; forcing THP needs
kernel settings, which rule 11 forbids), a startup page-cache prefetch (the
scoring run warms up first, so it is neutral), and a `timerfd` sweep (the existing
1 s `epoll_wait` timeout already wakes only when idle - no extra wake-ups to
remove).
