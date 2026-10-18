# Performance

This document describes what was measured, how to reproduce it, and what the numbers do and do not mean.

## What the challenge measures

The scored workload is a five-minute k6 hold. A run passes when:

- p95 latency is below 500 ms
- p99 latency is below 1000 ms
- the failed-request rate stays below 1%
- the highest passing VU level is the score

The 64k paced mixed test is a separate stress test. It is useful for finding CPU, memory, WAL, and connection-lifetime problems, but it is not the official score.

## How to reproduce

From the submission directory:

```bash
bash build.sh
bash test/run.sh submissions/nim-io_uring-NioHast
```

The integration harnesses are under `tests/`:

```bash
bash tests/feed_equality.sh
bash tests/commit_failure.sh
bash tests/conn_churn.sh
bash tests/conn_deferred_close.sh
bash tests/conn_lifetime_soak.sh
bash tests/fallback_matrix.sh
bash tests/multiring_smoke.sh
```

The official 42-check suite is byte-exact and runs against a fresh seed copy.

## Measured results

These numbers are from a local WSL2 development box with four vCPUs and about 3.8 GiB of RAM. The server and load generator share the machine over loopback. Absolute numbers are therefore not the official score.

### 64,000 paced mixed connections

| Metric | Result |
|---|---:|
| Requests completed | about 408,000 in 60 s |
| Throughput | about 6,800 req/s |
| p50 | about 0.5 ms |
| p95 | `32.768-49.152 ms` |
| p99 | `65.536-163.840 ms` |
| CPU | `59.7-60.8%` of one core |
| User CPU | `22.1-23.4%` |
| System CPU | `37.4-37.6%` |
| RSS | `178.7 MiB` |
| WAL max | `64 MiB` |
| Errors | `0` |

### 30-minute soak

32,000 connections at 3,400 req/s:

| Metric | Result |
|---|---:|
| p95 | `163.840 ms` |
| p99 | `327.680 ms` |
| Errors | `0` |
| CPU | `48.5%` of one core |
| RSS high-water mark | `156.2 MiB` |
| RSS drift from 60 s to end | `-13.28%` |
| WAL max | `102.4 MiB` |
| Worst off-loop checkpoint | `1.30 s` |

The checkpoint copy ran on the checkpoint thread. Request p99 stayed at `327.680 ms`.

### k6 hold

The direct-parse build passed these local runs:

| VUs | Duration | p95 | p99 | p99.9 | CPU | Errors |
|---:|---:|---:|---:|---:|---:|---:|
| 6,000 | 10 min | `10.43 ms` | `79.72 ms` | `186.10 ms` | `10.0%` | 0 |
| 10,000 | 5 min | `101.71 ms` | `213.57 ms` | `655.07 ms` | `12.7%` | 0 |
| 10,000 | 10 min | `85.23 ms` | `174.81 ms` | `436.58 ms` | `13.1%` | 0 |

A 15,000-VU local run collapsed on the k6 client: throughput fell to about 306 req/s while the server CPU stayed near 7%. That is a client limit, not a server saturation result.

## Why it is fast

- **Direct HTTP:** no reverse proxy, no extra network hop, and no proxy CPU.
- **One request thread:** one io_uring event loop, no worker pool, no thread hand-off.
- **Multishot receive and accept:** one operation stays armed across many completions.
- **Provided buffers and fixed files:** receives use a registered buffer ring, complete requests are parsed directly from the provided buffer, and only an incomplete tail is copied into the connection input buffer.
- **Fixed-file accepts:** accepts use kernel-allocated fixed-file slots.
- **Completion batching:** sustained load waits for a small batch, and one send can drain a pipelined output buffer.
- **Cheap TCP setup:** listener-level `TCP_DEFER_ACCEPT` and `TCP_NODELAY` are inherited by accepted sockets.
- **Prepared SQL:** statements are prepared once with `SQLITE_PREPARE_PERSISTENT`, with QPSG for plan stability.
- **One read transaction per pass:** all reads in a pass share a snapshot, and writes share a group commit.
- **Dense feed scan:** the feed uses a top-20 id window and range reads instead of a correlated count per row.
- **Direct writers:** feed and post responses write SQLite columns straight into the output buffer, with no `Post` object and no intermediate body string.
- **Small SQLite cache plus mmap:** `cache_size=-64` keeps SQLite's cache tiny, while `mmap_size=1 GiB` lets reads use the OS page cache.
- **Off-loop checkpoint:** a dedicated pthread with its own connection performs PASSIVE checkpoints, so requests do not pay the copy or fsync.
- **Borrowed request spans and stack JWT scratch:** parsing and auth do not copy the request or allocate per field.
- **SSE2 JSON work:** escaping scans 16 bytes at a time, clean runs are copied in bulk, and integer formatting uses a two-digit lookup.
- **PGO, LTO, native code:** the build uses profile-guided optimisation, link-time optimisation, and `-march=native`.
- **Pooled output buffers:** idle connections hold no receive buffer, and output buffers are returned after the terminal send.

## Limitations

- The official score comes from the organiser's k6 run on a 1 vCPU / 2 GB droplet with separate load machines. Local loopback numbers are optimistic.
- The local box is four vCPUs. IO and scheduler behaviour differ from the target droplet.
- At 64k, system CPU is higher than the Rust implementation. The remaining cost is mostly kernel/network work, not application work.
- WAL growth is the price of the off-loop PASSIVE checkpoint. A saturated 64k writer can grow the WAL to roughly 64-225 MiB in a minute. A five-minute 64k hold should have about 1 GiB of free disk headroom. This is disk growth, not memory growth.
- RSS grows with live connections, not with the WAL file. The WAL is on disk and is not mmap'd as one giant allocation.
- The 10,000-VU result is stable on the local client; the 15,000-VU run is client-limited. The official score still comes from a separate organiser load generator.
- The numbers above are local measurements, not a guarantee of the official score.
