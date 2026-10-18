## Unit test for src/db.nim against a copy of the challenge seed database.
##
## The seed DB is never touched: we copy it to a temp file and open the copy.
## Each case prints "PASS <name>"; the first failure prints "FAIL <name>" and
## exits non-zero.

import std/[json, os, strutils]
import ../src/db
import ../src/checkpoint
import ../src/model

var failures = 0

proc pass(name: string) =
  echo "PASS ", name

proc fail(name, detail: string) =
  inc failures
  echo "FAIL ", name, ": ", detail
  quit(1)

proc expect(name: string, cond: bool, detail: string) =
  if cond: pass(name) else: fail(name, detail)

proc runTests() =
  # locate the seed DB relative to this source file, copy it to a temp file.
  let testDir = currentSourcePath().parentDir               # .../tests
  let submissionDir = testDir.parentDir                     # .../nim-io_uring-NioHast
  let defaultSeed = submissionDir.parentDir.parentDir / "seed" / "feed.db"
  let seedDb = getEnv("SEED_DB", defaultSeed)

  if not fileExists(seedDb):
    quit("seed db not found: " & seedDb, 1)

  let tmpDb = getTempDir() / ("feed_test_" & $getCurrentProcessId() & ".db")
  copyFile(seedDb, tmpDb)
  echo "# seed   = ", seedDb
  echo "# copy   = ", tmpDb

  defer:
    for suffix in ["", "-wal", "-shm"]:
      let f = tmpDb & suffix
      if fileExists(f):
        try: removeFile(f) except OSError: discard

  let db = openDb(tmpDb)

  # health ------------------------------------------------------------------
  expect("health", db.health(), "health() returned false")

  # feed: newest 20 ---------------------------------------------------------
  # `writeFeedJson` is the sole production path; the ordered-query writer is its
  # decline path and doubles as the byte-equality oracle here.
  var feedBody = ""
  db.writeFeedJson(feedBody)
  var orderedBody = ""
  db.writeFeedFallbackJson(orderedBody)
  expect("feed_bytes_match_ordered", feedBody == orderedBody,
         "dense writer diverged from the ordered query")
  let feed = parseJson(feedBody)
  echo "# feed[0].id = ", feed["posts"][0]["id"].getInt,
       " (len=", feed["posts"].len, ")"
  expect("feed_len_20", feed["posts"].len == 20, "len=" & $feed["posts"].len)
  expect("feed_first_id_500000", feed["posts"][0]["id"].getInt == 500000,
         "id=" & $feed["posts"][0]["id"].getInt)

  # getPost(1) --------------------------------------------------------------
  var p1: Post
  let found1 = db.getPost(1, p1)
  echo "# post1 author=", p1.author, " likes=", p1.likeCount,
       " created=", p1.createdAt
  expect("getPost_1_found", found1, "getPost(1) returned false")
  expect("getPost_1_author", p1.author == "happy_nebula_10241",
         "author=" & p1.author)
  expect("getPost_1_likeCount", p1.likeCount == 11, "likeCount=" & $p1.likeCount)
  expect("getPost_1_createdAt",
         p1.createdAt.startsWith("2025-01-01T00:00:34"),
         "createdAt=" & p1.createdAt)

  # getPost(missing) --------------------------------------------------------
  var missing: Post
  let foundMissing = db.getPost(999999999'i64, missing)
  expect("getPost_missing_false", not foundMissing,
         "getPost(999999999) returned true")

  # createPost --------------------------------------------------------------
  var created: Post
  let createdOk = db.createPost(1, "golden_ember_1", "hello", created)
  echo "# created id=", created.id, " createdAt=", created.createdAt
  expect("createPost_true", createdOk, "createPost returned false")

  var reread: Post
  let rereadOk = db.getPost(created.id, reread)
  expect("createPost_reread_found", rereadOk,
         "getPost(new id) returned false")
  expect("createPost_body", reread.body == "hello", "body=" & reread.body)
  expect("createPost_likeCount", reread.likeCount == 0,
         "likeCount=" & $reread.likeCount)
  expect("createPost_author", reread.author == "golden_ember_1",
         "author=" & reread.author)

  # like --------------------------------------------------------------------
  let like1 = db.like(1, 1)
  echo "# like(1,1) first = ", like1
  expect("like_first_liked", like1 == loLiked, "got " & $like1)

  let like2 = db.like(1, 1)
  echo "# like(1,1) second = ", like2
  expect("like_second_already", like2 == loAlready, "got " & $like2)

  let like3 = db.like(1, 999999999'i64)
  echo "# like(1,missing) = ", like3
  expect("like_missing_nopost", like3 == loNoPost, "got " & $like3)

  # --- per-pass read transaction -------------------------------------------
  # From here on group commit is ON, so `commitBatch` is the pass boundary.
  db.setGroupCommit(true)

  # read-then-write in one pass: `writeFeedJson` opens the pass's deferred read
  # transaction and `createPost` must end it before `BEGIN IMMEDIATE` instead of
  # raising "cannot start a transaction within a transaction".
  db.writeFeedJson(feedBody)
  var rwPost: Post
  let rwOk = db.createPost(1, "golden_ember_1", "read then write", rwPost)
  expect("read_then_write_create", rwOk,
         "createPost after feed returned false")
  expect("read_then_write_commit", db.commitBatch(),
         "commitBatch after read-then-write returned false")
  expect("read_then_write_read_txn_ended", db.endRead(),
         "endRead after a read->write pass did not report a closed transaction")

  # fresh-read visibility after a write commit: the next pass opens a NEW read
  # transaction and must see the committed row. A snapshot carried across the
  # boundary would hide it.
  var seen: Post
  let seenOk = db.getPost(rwPost.id, seen)
  expect("fresh_read_after_commit",
         seenOk and seen.body == "read then write",
         "new pass did not see the committed post")

  # read -> write in the same pass again, this time a like, then commit.
  db.writeFeedJson(feedBody)
  let lk = db.like(1, rwPost.id)
  expect("read_then_like_liked", lk == loLiked, "got " & $lk)
  expect("read_then_like_commit", db.commitBatch(),
         "commitBatch after read-then-like returned false")

  # --- checkpoint lifecycle ------------------------------------------------
  expect("read_pass_boundary_end", db.endRead(),
         "endRead at the pass boundary failed")

  # commit-after-start (hook path): start the worker, then make a REAL commit.
  # The WAL hook on the request connection must raise the flag and the worker
  # must run a checkpoint on its own second connection. Poll the live stats
  # BEFORE stopping, so a final shutdown checkpoint cannot mask a dead hook.
  db.startCheckpointer()
  var ckPost: Post
  expect("checkpoint_commit_create",
         db.createPost(1, "golden_ember_1", "checkpoint hook", ckPost),
         "createPost before checkpointing returned false")
  expect("checkpoint_commit_commit", db.commitBatch(),
         "commitBatch before checkpointing returned false")
  var hookStats = db.checkpointStats()
  var hookRan = false
  for _ in 0 ..< 200:
    hookStats = db.checkpointStats()
    if hookStats.count >= 1 and hookStats.last_pn_log > 0:
      hookRan = true
      break
    sleep(10)
  echo "# ckpt hook: count=", hookStats.count, " pnLog=",
       hookStats.last_pn_log, " pnCkpt=", hookStats.last_pn_ckpt,
       " busy=", hookStats.busy, " errors=", hookStats.errors
  expect("checkpoint_hook_ran_after_commit",
         hookRan and hookStats.count >= 1 and hookStats.last_pn_log > 0 and
           hookStats.errors == 0,
         "count=" & $hookStats.count & " pnLog=" & $hookStats.last_pn_log &
           " errors=" & $hookStats.errors)
  expect("checkpoint_hook_busy_zero", hookStats.busy == 0,
         "busy=" & $hookStats.busy)

  # The explicit signal path (used by tests and shutdown) must wake the worker
  # without a new commit. The worker coalesces to one checkpoint per interval,
  # so allow up to 3 s for the count to advance.
  let manualBefore = db.checkpointStats().count
  db.requestCheckpoint()
  var manualStats = db.checkpointStats()
  var manualRan = false
  for _ in 0 ..< 300:
    manualStats = db.checkpointStats()
    if manualStats.count > manualBefore:
      manualRan = true
      break
    sleep(10)
  expect("manual_checkpoint_ran",
         manualRan and manualStats.errors == 0,
         "count " & $manualBefore & " -> " & $manualStats.count &
           ", errors=" & $manualStats.errors)

  db.stopCheckpointer()
  let finalStats = db.checkpointStats()
  echo "# ckpt final: count=", finalStats.count, " pnLog=",
       finalStats.last_pn_log, " errors=", finalStats.errors
  expect("checkpoint_stats_after_stop",
         finalStats.count >= 1 and finalStats.last_pn_log > 0 and
           finalStats.errors == 0 and finalStats.failed == 0,
         "count=" & $finalStats.count & " pnLog=" & $finalStats.last_pn_log &
           " errors=" & $finalStats.errors & " failed=" & $finalStats.failed)

  # stop is single-owner/idempotent and the cached stats survive a second call.
  db.stopCheckpointer()
  let secondStopStats = db.checkpointStats()
  expect("checkpoint_stop_idempotent",
         secondStopStats.count == finalStats.count and
           secondStopStats.errors == finalStats.errors,
         "second stop changed the cached stats")

  var afterCkpt: Post
  let afterOk = db.getPost(ckPost.id, afterCkpt)
  expect("checkpoint_after_read_pass",
         afterOk and afterCkpt.body == "checkpoint hook",
         "post unreadable after the checkpoint")

  # write-after-stop: the hook must be uninstalled before the context is freed.
  # This COMMIT would call ckSignal() on freed memory if the context were freed first.
  var afterStopPost: Post
  let writeAfterStop =
    db.createPost(1, "golden_ember_1", "write after stop", afterStopPost)
  let commitAfterStop = db.commitBatch()
  expect("write_after_stop", writeAfterStop and commitAfterStop,
         "createPost/commit after stopCheckpointer failed")
  var afterStopRead: Post
  let afterStopReadOk = db.getPost(afterStopPost.id, afterStopRead)
  expect("write_after_stop_readable",
         afterStopReadOk and afterStopRead.body == "write after stop",
         "DB unreadable after a write following stopCheckpointer")
  db.writeFeedJson(feedBody)
  expect("write_after_stop_feed_read", db.endRead(),
         "feed read after stop did not close its transaction")

  # stop-with-pending: commit and stop immediately, before the worker has
  # necessarily consumed the signal. The shutdown path must still run the
  # wanted PASSIVE checkpoint and report it.
  db.startCheckpointer()
  var pendingPost: Post
  expect("stop_pending_create",
         db.createPost(1, "golden_ember_1", "stop with pending", pendingPost),
         "createPost before pending stop returned false")
  expect("stop_pending_commit", db.commitBatch(),
         "commitBatch before pending stop returned false")
  db.stopCheckpointer()
  let pendingStats = db.checkpointStats()
  echo "# ckpt pending: count=", pendingStats.count, " pnLog=",
       pendingStats.last_pn_log, " errors=", pendingStats.errors
  expect("checkpoint_stop_with_pending",
         pendingStats.count >= 1 and pendingStats.last_pn_log > 0 and
           pendingStats.errors == 0 and pendingStats.failed == 0,
         "count=" & $pendingStats.count & " pnLog=" &
           $pendingStats.last_pn_log & " errors=" & $pendingStats.errors)

  # concurrent reader/writer loop while the checkpointer runs. Each pass reads
  # the feed and commits a new post; the worker checkpoints in parallel.
  db.startCheckpointer()
  var loopFailures = 0
  var sawConcurrentCheckpoint = false
  for i in 0 ..< 60:
    db.writeFeedJson(feedBody)
    var p: Post
    if not db.createPost(1, "golden_ember_1", "concurrent " & $i, p):
      inc loopFailures
    if not db.commitBatch():
      inc loopFailures
    let live = db.checkpointStats()
    if live.count >= 1:
      sawConcurrentCheckpoint = true
    sleep(5)
  # Give an asynchronously scheduled worker a bounded chance to be observed
  # BEFORE stopping, so this proves concurrency rather than a shutdown flush.
  for _ in 0 ..< 100:
    if db.checkpointStats().count >= 1:
      sawConcurrentCheckpoint = true
      break
    sleep(10)
  let liveStats = db.checkpointStats()
  expect("checkpoint_concurrent_ran",
         sawConcurrentCheckpoint and liveStats.count >= 1,
         "worker never checkpointed during the concurrent loop")
  expect("checkpoint_concurrent_no_errors",
         loopFailures == 0 and liveStats.errors == 0 and liveStats.failed == 0,
         "loopFailures=" & $loopFailures & " errors=" & $liveStats.errors)
  db.stopCheckpointer()
  let concurrentFinal = db.checkpointStats()
  echo "# ckpt concurrent: count=", concurrentFinal.count, " busy=",
       concurrentFinal.busy, " errors=", concurrentFinal.errors
  expect("checkpoint_concurrent_stop_clean",
         concurrentFinal.errors == 0 and concurrentFinal.failed == 0,
         "errors=" & $concurrentFinal.errors & " failed=" & $concurrentFinal.failed)
  var lastPost: Post
  let lastPostOk = db.getPost(pendingPost.id, lastPost)
  expect("db_readable_after_concurrent_stop",
         lastPostOk and lastPost.body == "stop with pending",
         "DB unreadable after the concurrent checkpoint run")

  # --- checkpoint edge cases ----------------------------------------------
  # start -> immediate stop: the worker can be created and joined before it
  # ever checkpoints. It must exit cleanly with no pending work and no errors.
  db.startCheckpointer()
  db.stopCheckpointer()
  let immediateStats = db.checkpointStats()
  expect("checkpoint_start_immediate_stop",
         immediateStats.errors == 0 and immediateStats.failed == 0,
         "errors=" & $immediateStats.errors & " failed=" &
           $immediateStats.failed)

  # repeated lifecycle: 50 complete start/commit/stop cycles on one Db. Each
  # stop must join the worker, cache the final stats and leave the connection
  # reusable for the next cycle (no leaked hook/context/thread).
  var lifecycleFailures = 0
  for i in 0 ..< 50:
    db.startCheckpointer()
    var cyclePost: Post
    if not db.createPost(1, "golden_ember_1", "lifecycle " & $i, cyclePost):
      inc lifecycleFailures
    if not db.commitBatch():
      inc lifecycleFailures
    db.stopCheckpointer()
    let cycleStats = db.checkpointStats()
    if cycleStats.errors != 0 or cycleStats.failed != 0:
      inc lifecycleFailures
  expect("checkpoint_50x_start_commit_stop",
         lifecycleFailures == 0, "failures=" & $lifecycleFailures)

  # coalescing: 100 commits in one burst must not produce 100 checkpoints.
  # The worker runs at most one PASSIVE checkpoint per 1 s interval; after a
  # 2.5 s window the count delta must be at most 2 (and at least 1).
  db.startCheckpointer()
  let beforeBurst = db.checkpointStats().count
  var burstFailures = 0
  for i in 0 ..< 100:
    var burstPost: Post
    if not db.createPost(1, "golden_ember_1", "burst " & $i, burstPost):
      inc burstFailures
    if not db.commitBatch():
      inc burstFailures
  let liveAfterBurst = db.checkpointStats()
  sleep(2500)
  let afterWindowStats = db.checkpointStats()
  let burstDelta = afterWindowStats.count - beforeBurst
  echo "# ckpt coalesce: before=", beforeBurst, " after=", afterWindowStats.count,
       " delta=", burstDelta, " live_errors=", liveAfterBurst.errors
  expect("checkpoint_100_commits_coalesced",
         burstFailures == 0 and burstDelta >= 1 and burstDelta <= 2 and
           afterWindowStats.errors == 0 and afterWindowStats.failed == 0,
         "failures=" & $burstFailures & " delta=" & $burstDelta &
           " errors=" & $afterWindowStats.errors)
  db.stopCheckpointer()
  let coalesceFinal = db.checkpointStats()
  expect("checkpoint_coalesce_stop_clean",
         coalesceFinal.errors == 0 and coalesceFinal.failed == 0,
         "errors=" & $coalesceFinal.errors & " failed=" & $coalesceFinal.failed)

  # An unopenable checkpoint path must make ckStart return NULL. db.nim maps
  # that NULL to a DbError, so the server can never start with a WAL that no
  # worker will checkpoint.
  let badCtx = ckStart(cstring("/tmp/opencode/no-such-b4-dir/feed.db"), 1000)
  expect("checkpoint_invalid_path_nil", badCtx == nil,
         "ckStart returned a context for an unopenable path")

runTests()

if failures == 0:
  echo "ALL TESTS PASSED"
else:
  echo "FAILURES: ", failures
  quit(1)
