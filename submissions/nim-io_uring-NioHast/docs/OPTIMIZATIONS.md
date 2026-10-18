# Performance Optimizations

This document describes the performance design of the server, including build-time and runtime techniques. Each entry explains what the technique does and where it is implemented.

## Transport and event loop

| Technique | What it does | Source |
|---|---|---|
| Direct HTTP/1.1 | The server binds directly and serves HTTP without a reverse proxy, avoiding proxy CPU and an extra network hop. | `src/server.nim`, `src/http.nim` |
| One request thread | A single event loop handles all sockets, so there is no worker pool, thread hand-off, or per-request thread scheduling. | `src/server.nim` |
| io_uring transport | Networking uses completion-based io_uring operations instead of an epoll plus read/write loop. | `src/iouring.nim`, `src/server.nim` |
| Multishot recv | One receive operation stays armed across many completions, reducing submission overhead. | `src/server.nim` |
| Provided-buffer ring | A registered buffer ring supplies receive buffers, so a recv does not allocate or copy a new buffer each time. | `src/server.nim`, `src/iouring.nim` |
| Direct request parsing | Complete requests are parsed and served in place from the provided receive buffer, including pipelined requests in one buffer. Only an incomplete request is copied into the connection input buffer. | `src/server.nim`, `src/http.nim` |
| Multishot accept | One accept operation serves a stream of incoming connections instead of re-arming after every accept. | `src/server.nim` |
| Direct accept | The kernel allocates a fixed-file slot for each accepted connection, avoiding a per-accept registration syscall. | `src/server.nim` |
| Fixed-file close | `IORING_OP_CLOSE` closes the fixed descriptor directly, avoiding a separate close or register-update round trip. | `src/server.nim` |
| `IOSQE_CQE_SKIP_SUCCESS` | Fire-and-forget operations do not post a success completion, so the loop processes fewer CQEs. | `src/server.nim` |
| `IORING_SETUP_NO_SQARRAY` | The kernel indexes submission entries directly by tail, removing one indirection on every submission. | `src/server.nim`, `src/iouring.nim` |
| `SINGLE_ISSUER` | The ring is created and entered by one thread, allowing the kernel to skip cross-thread submission checks. | `src/server.nim` |
| `DEFER_TASKRUN` | Task work runs only at an explicit `io_uring_enter`, reducing wakeups in the single-ring path. | `src/server.nim` |
| `COOP_TASKRUN` | Task work cooperates with the request thread instead of using an extra task-work wakeup. | `src/server.nim` |
| CQ headroom | The completion queue is sized larger than the submission queue, so bursts do not overflow the CQ. | `src/server.nim` |
| SQ/CQ overflow handling | The loop detects full rings and recovers without dropping completions or stalling. | `src/iouring.nim`, `src/server.nim` |
| Completion batch wait | Under sustained load the loop waits for up to eight completions with a 100 microsecond cap. | `src/server.nim` |
| Enter batching | One `io_uring_enter` submits and waits for a whole batch, amortising syscall cost. | `src/server.nim` |
| One SEND per drained output | A pipelined batch is sent in one operation where possible; a partial send re-arms the remainder. | `src/server.nim` |
| Buffer-tail batching | The provided-buffer tail is published once per batch instead of once per buffer. | `src/server.nim` |
| Listener TCP options | `TCP_DEFER_ACCEPT` and `TCP_NODELAY` are set once on the listener and inherited by accepted sockets. | `src/server.nim` |
| Idle sweep | A one-second timer closes idle and stalled connections, so no per-connection timer is needed. | `src/server.nim` |
| Multi-ring sizing | The server adds rings and fixed-file tables instead of threads when the connection limit requires more capacity. | `src/server.nim` |
| Compact `Conn` | A connection slot is 80 bytes, and the slot array grows lazily as connections arrive. | `src/server.nim` |
| Output-buffer pool | Up to 128 output buffers are pooled and returned after the terminal send; large buffers are not retained. | `src/server.nim` |
| No recv buffer per idle connection | Receive buffers come from the provided-buffer ring and are returned when not in use. | `src/server.nim` |
| Deferred SEND until COMMIT | A response produced inside a write batch is withheld until the batch commits, so no success byte can precede durable data. | `src/server.nim`, `src/db.nim` |

## SQLite and checkpoint

| Technique | What it does | Source |
|---|---|---|
| WAL mode | Readers use a snapshot while one writer appends to the WAL, so reads and writes do not block each other. | `src/db.nim` |
| `synchronous=NORMAL` | Avoids a full fsync on every commit while keeping the database consistent. | `src/db.nim` |
| `busy_timeout=5000` | Bounds how long a statement waits for a lock instead of failing immediately. | `src/db.nim` |
| `foreign_keys=ON` | Enforces the schema's referential integrity on every connection. | `src/db.nim` |
| `temp_store=MEMORY` | Keeps temporary B-trees in memory instead of writing them to disk. | `src/db.nim` |
| `cache_size=-64` | Keeps SQLite's own page cache small and relies on the OS page cache for database pages. | `src/db.nim` |
| `mmap_size=1 GiB` | Maps the main database file so reads can come from the OS page cache without a copy. | `src/db.nim` |
| `journal_size_limit=64 MiB` | Caps how much WAL space is retained after a checkpoint. | `src/db.nim` |
| `wal_autocheckpoint=0` | SQLite does not checkpoint on the request thread; the checkpoint worker owns that work. | `src/db.nim` |
| Off-loop PASSIVE checkpoint | A separate SQLite connection and pthread copy the WAL, so the request loop never performs a checkpoint. | `src/checkpoint.nim` |
| Coalesced checkpoint signals | Commits only wake the worker; the worker coalesces work to at most one checkpoint per second. | `src/checkpoint.nim`, `src/db.nim` |
| Raw-pthread worker | The worker uses a Nim pthread with atomics and stack buffers, avoiding the Nim runtime on the worker thread. | `src/checkpoint.nim` |
| Persistent prepared statements | SQL is parsed once with `SQLITE_PREPARE_PERSISTENT`, so the hot path reuses prepared statements. | `src/db.nim` |
| QPSG | Query-plan stability is enabled so a re-prepared statement keeps the intended plan. | `src/db.nim`, `src/sqlite3.nim` |
| One read transaction per pass | All reads in an event-loop pass share one snapshot, reducing transaction and lock churn. | `src/db.nim`, `src/server.nim` |
| Group commit | All writes in one pass share one transaction, so many requests amortise one COMMIT. | `src/db.nim`, `src/server.nim` |
| Commit before SEND | A successful response is sent only after its transaction commits. | `src/server.nim`, `src/db.nim` |
| Dense feed scan | The top-20 ids define a small id window; likes and rows are fetched by range instead of running a correlated count for each row. | `src/db.nim` |
| Ordered feed fallback | If the window is sparse or rows do not line up, the original ordered query runs instead. | `src/db.nim` |
| Direct feed writer | SQLite columns are written straight into the response buffer, with no `Post` sequence and no intermediate body string. | `src/db.nim`, `src/jsonbuf.nim` |
| Direct post writer | `GET /posts/:id` serialises columns directly instead of building a `Post` object first. | `src/db.nim` |
| Targeted `ON CONFLICT` | `ON CONFLICT(user_id, post_id) DO NOTHING` avoids a broad conflict search for duplicate likes. | `src/db.nim` |
| Health without a read transaction | `SELECT 1` does not open the pass read transaction, so health-only passes skip BEGIN/COMMIT. | `src/db.nim` |
| Minimal statement resets | Statements are reset only where required; redundant binding clears were removed. | `src/db.nim` |
| Prepared transaction control | BEGIN, COMMIT, and ROLLBACK are prepared once and reused. | `src/db.nim` |
| SQLite-only data path | Every request reads SQLite; there is no in-process response cache, query cache, or table copy to invalidate. | `src/db.nim` |
| SQLite compile trims | Unused SQLite features are omitted and QPSG is enabled to keep the library smaller and focused. | `src/sqlite3.nim`, `build.sh` |

## HTTP, JSON, and JWT

| Technique | What it does | Source |
|---|---|---|
| O(n) cursor parser | The parser walks the request buffer with offsets instead of deleting or shifting bytes. | `src/http.nim` |
| Borrowed request spans | Headers, body, and JWT fields point into the request buffer instead of being copied. | `src/http.nim`, `src/jwt.nim` |
| Canonical fast paths | The exact official header and body shapes bypass `std/json`; other valid JSON uses the standard parser. | `src/jwt.nim`, `src/http.nim` |
| Fixed `JwtScratch` | Header, payload, and signature decoding use a fixed stack buffer, avoiding per-request allocations. | `src/jwt.nim` |
| SSE2 JSON escaper | The escaper scans 16 bytes at a time for quote, backslash, and control characters. | `src/jsonbuf.nim` |
| Bulk clean-run copy | Long runs with no escapes are copied in one move instead of byte by byte. | `src/jsonbuf.nim` |
| Two-digit itoa lookup | Integer formatting writes two decimal digits per step, reducing division and branches. | `src/jsonbuf.nim` |
| SSE2 CRLF finder | The header terminator is found 16 bytes at a time, with overlap for boundary matches. | `src/http.nim` |
| 128-byte head reserve | The response head and body share one buffer; the body moves into place without a second allocation. | `src/db.nim` |
| No intermediate response strings | Feed and post bodies are serialised directly into the output buffer. | `src/db.nim`, `src/jsonbuf.nim` |
| Keep-alive and pipelining | Multiple requests share a connection, and pipelined requests are parsed without per-request connection setup. | `src/http.nim`, `src/server.nim` |
| Output cap | A bounded output buffer prevents deep pipelines from growing memory without limit. | `src/server.nim` |

## Build and runtime

| Technique | What it does | Source |
|---|---|---|
| Two-pass PGO | The build runs an instrumented pass, trains on a workload, then rebuilds with profile data. | `build.sh` |
| SQLite PGO | The SQLite C source is compiled in both PGO passes, so hot SQLite paths are optimised too. | `build.sh` |
| LTO | Link-time optimisation removes cross-unit overhead in the final build. | `build.sh` |
| Native code generation | `-O3 -march=native` targets the host CPU's instruction set. | `build.sh` |
| Direct external calls | `-fno-semantic-interposition` and `-fno-plt` remove avoidable indirection for external calls. | `build.sh` |
| ARC with threads off | `--mm:arc --threads:off` keeps GC and thread-runtime work out of the request path. | `build.sh` |
| Checks retained | The default build keeps bounds and overflow checks; the unsafe variant is not used for the submission. | `build.sh` |
| Parallel build | `NIM_JOBS` lets the compiler use multiple cores during the build. | `build.sh` |
| Vendored dependencies | SQLite and nimcrypto are vendored and checksum-verified, so the build has no external service dependency. | `build.sh`, `config.nims` |

## Robustness and lifetime

| Technique | What it does | Source |
|---|---|---|
| Slot generations | A stale completion cannot free or modify a slot that has been reused by a new connection. | `src/server.nim` |
| Close after pending operations | A slot is released only after its last in-flight operation completes. | `src/server.nim` |
| Direct-slot recovery | If a fixed close cannot be submitted, a register update drops the kernel slot so it is not leaked. | `src/server.nim` |
| ENOBUFS and partial-send recovery | The loop re-arms or retries instead of dropping the connection when buffers or send space run out. | `src/server.nim` |
| Deferred close handling | Buffered output is drained before a connection is closed. | `src/server.nim` |
| Deferred batch rollback | Withheld responses become 500 responses if the transaction fails to commit. | `src/server.nim`, `src/db.nim` |
| WAL startup check | Startup fails if SQLite cannot enable WAL, instead of running with checkpoint assumptions that no longer hold. | `src/db.nim` |
