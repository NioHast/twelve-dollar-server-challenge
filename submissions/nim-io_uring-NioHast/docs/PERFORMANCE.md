# Performance

This document describes the current measurements, how to reproduce them, and what the numbers do and
do not mean.

## What the challenge measures

The scored workload is a five-minute k6 hold. A run passes when:

- p95 latency is below 500 ms
- p99 latency is below 1000 ms
- the failed-request rate stays below 1%
- the highest passing VU level is the score

The 64k paced mixed test is a separate stress test. It is useful for finding CPU, memory, WAL, and
connection-lifetime problems, but it is not the official score.

## How to reproduce

From the submission directory:

```bash
bash build.sh
bash test/run.sh submissions/nim-io_uring-NioHast
```

The integration harnesses are under `tests/`:

```bash
bash tests/feed_equality.sh
bash tests/conn_churn.sh
bash tests/conn_deferred_close.sh
bash tests/conn_lifetime_soak.sh
bash tests/fallback_matrix.sh
bash tests/multiring_smoke.sh
```

The official 42-check suite is byte-exact and runs against a fresh seed copy.

The k6 figure below uses `bench/load.js` with 5,000 VUs, a 30 s ramp and a 120 s hold. The server was
pinned to CPU 0 for that run.

## Measured results

These numbers are from a local WSL2 development box with four vCPUs. The 64k figures are same-session
means of four repetitions. The server and load generator share the host over loopback, so absolute
numbers are not the official score.

### 64,000 paced mixed connections

| Metric | Result |
|---|---:|
| Throughput | about 6,800 req/s |
| CPU | `62.2%` of one core |
| User CPU | `18.25%` |
| System CPU | `43.95%` |
| p99 | `41-229 ms` |
| RSS | `142.8 MiB` |
| WAL max | `16.2 MiB` |
| Errors | `0` |

Across the four runs, p99 ranged from 41 to 229 ms (median about 86 ms). The high end is a
pass-boundary checkpoint stall. Every run stayed below the 1 s p99 threshold with zero errors.

### k6 hold

5,000 VUs, 30 s ramp + 120 s hold:

| Metric | Result |
|---|---:|
| CPU | `8.3%` of one core |
| User CPU | `3.0%` |
| System CPU | `5.25%` |
| p95 | `4.46 ms` |
| p99 | `14.1 ms` |
| Errors | `0` |

The local load generator is co-located with the server, so this run cannot certify a maximum user
count. It is a local regression check, not an organiser score.

## Why it is fast

- **Direct HTTP:** no reverse proxy, no extra network hop, and no proxy CPU.
- **One request thread:** one io_uring event loop, no worker pool, no thread hand-off.
- **One SQLite connection:** the inline checkpoint runs on the same connection and thread, so there
  is no cross-thread coordination.
- **Multishot receive and accept:** one operation stays armed across many completions.
- **Provided buffers and fixed files:** receives use a registered buffer ring (256 buffers of 8 KiB by
  default), complete requests are parsed directly from the provided buffer, and only an incomplete
  tail is copied into the connection input buffer.
- **Fixed-file accepts:** accepts use kernel-allocated fixed-file slots.
- **Completion batching:** sustained load waits for a small batch, and one send can drain a pipelined
  output buffer.
- **Cheap TCP setup:** listener-level `TCP_DEFER_ACCEPT` and `TCP_NODELAY` are inherited by accepted
  sockets.
- **Prepared SQL:** statements are prepared once with `SQLITE_PREPARE_PERSISTENT`, with QPSG for
  stability.
- **One read transaction per pass:** all reads in a pass share a snapshot, and writes share a group
  commit.
- **Top-20 feed ids plus rowid range:** the feed reads the top-20 ids through the `created_at` index,
  counts likes inside that id window, then fetches rows with `id BETWEEN ? AND ?` and reassembles them
  in memory, instead of running a correlated count per row.
- **Direct writers:** feed rows are staged in a reusable `feedScratch` buffer and copied into the
  output buffer with `copyMem`; there is no `Post` object, no per-row temporary string and no
  intermediate body string.
- **Small SQLite cache plus mmap:** `cache_size=-64` keeps SQLite's cache tiny, while `mmap_size=1 GiB`
  lets reads use the OS page cache.
- **Inline RESTART checkpoint:** the single connection runs one `SQLITE_CHECKPOINT_RESTART` at the
  pass boundary when the WAL hook reaches 4096 frames, so the WAL stays around 16 MiB.
- **Borrowed request spans and stack JWT scratch:** parsing and auth do not copy the request or
  allocate per field.
- **SSE2 JSON work:** escaping scans 16 bytes at a time, clean runs are copied in bulk, and integer
  formatting uses a two-digit lookup.
- **Danger, PGO, LTO, native code:** the shipping build uses `-d:danger`, profile-guided optimisation,
  link-time optimisation, and `-march=native`.
- **Pooled output buffers:** idle connections hold no receive buffer, and output buffers are returned
  after the terminal send.

## Limitations

- The official score comes from the organiser's k6 run on a 1 vCPU / 2 GB droplet with separate load
  machines. Local loopback numbers are optimistic and are not organiser numbers.
- The local measurements share one host with the load generator. The 1 vCPU / 2 GB target needs a
  separate load generator for a true maximum-users score.
- `locking_mode=EXCLUSIVE` means the commit-failure harness is not applicable. The
  42-check suite is the CI gate, and durability was verified with a 3,000-post SIGTERM and reopen
  check.
- The shipping `-d:danger` build removes Nim bounds, overflow and range checks and `assert`. The
  42-check suite and the 64k load test pass, but malformed-input fuzzing is still advisable.
- RSS grows with live connections, not with the WAL file. The WAL is on disk and is not part of
  process RSS.
- The numbers above are local measurements, not a guarantee of the official score.
