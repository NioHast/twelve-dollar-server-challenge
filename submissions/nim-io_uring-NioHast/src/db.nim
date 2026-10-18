## Data-access layer. Owns every SQL statement in the submission.
##
## Uses the raw bindings from `sqlite3` directly (no std/db_sqlite, no ORM).
## Statements are prepared once at open time with SQLITE_PREPARE_PERSISTENT and
## reused; each call resets and re-binds.
##
## Read transactions (one per event-loop pass)
## --------------------------------------------
## The first read of a pass opens ONE `BEGIN DEFERRED` (prepared once, stepped
## like any other statement); every read statement of that pass runs inside it
## and the server COMMITs it at the pass boundary (`endRead`). Without this,
## each statement ran in its own implicit transaction and `sqlite3_reset` ended
## it: the old feed scan alone paid three transaction setups per request. The
## explicit transaction is held only until the end of the pass -- never across
## passes and never across the WAL checkpoint -- so the connection always sees writes that
## committed during an earlier pass and PASSIVE checkpointing never has to skip
## frames because of a stale reader snapshot.
##
## Invariants:
##   (a) `readOpen` is false while a write batch is open. `beginBatch` ends the
##       read transaction before `BEGIN IMMEDIATE`, so a read-then-write pass
##       never nests transactions and an autocommit write (group commit off)
##       never silently joins the read snapshot.
##   (b) the read transaction is ended at the pass boundary (`endRead`), before
##       the checkpointer can backfill the frames that transaction still pins.
##       The checkpoint itself runs on its OWN connection/thread (see below), so
##       no SQLite call for it ever runs between a request's reads/writes.
##   (c) every read statement is reset after use, so no statement is left
##       ACTIVE at the pass boundary; reset no longer ends the read transaction
##       (the explicit COMMIT does), it only makes the statement reusable.
##
## Write batching (group commit)
## -----------------------------
## By default (`setGroupCommit` off) every write runs in autocommit mode, so it
## is durable before the proc returns. When group commit is enabled the server
## opens ONE `BEGIN IMMEDIATE` per event-loop pass (the first write of the pass
## calls `beginBatch`) and `COMMIT`s it once, at the end of the pass. Writes and
## reads executed while the batch is open all run inside that single
## transaction, so a read issued after a write in the same pass observes the
## write, and the whole pass becomes durable at one commit.
##
## IMPORTANT: no SAVEPOINT is ever used. Ordinary constraint failures must not
## tear the batch, and the write statements are written so they cannot fail on a
## constraint: `insert_like` is `INSERT ... SELECT ... WHERE EXISTS(...)
## ON CONFLICT(user_id, post_id) DO NOTHING` (a missing post inserts zero rows, a
## duplicate inserts zero rows) and `insert_post` has no unique constraint to
## violate. Any OTHER
## (unexpected) error aborts the whole batch via `abortBatch`/`commitBatch`
## rollback, and the server turns every response produced against that batch into
## a 500 -- it can never answer success for a write that did not commit.

import std/[os, strutils]

import ./sqlite3
import ./checkpoint
import ./model
import ./jsonbuf
import ./http

type
  DbError* = object of CatchableError

  BatchState = enum
    bsNone    ## no transaction open (autocommit)
    bsOpen    ## BEGIN IMMEDIATE issued, not yet committed
    bsFailed  ## the batch hit an unexpected error; COMMIT must report failure

  CheckpointStats* = CkStats

  Db* = ref object
    handle: ptr Sqlite3
    stmtGetPost: ptr Sqlite3Stmt
    stmtFeed: ptr Sqlite3Stmt
    stmtFeedIds: ptr Sqlite3Stmt
    stmtFeedLikes: ptr Sqlite3Stmt
    stmtFeedRows: ptr Sqlite3Stmt
    stmtInsertPost: ptr Sqlite3Stmt
    stmtCreatedAt: ptr Sqlite3Stmt
    stmtInsertLike: ptr Sqlite3Stmt
    stmtExistsPost: ptr Sqlite3Stmt
    stmtHealth: ptr Sqlite3Stmt
    stmtBeginRead: ptr Sqlite3Stmt
    stmtCommitRead: ptr Sqlite3Stmt
    stmtBeginBatch: ptr Sqlite3Stmt
    stmtCommitBatch: ptr Sqlite3Stmt
    stmtRollbackBatch: ptr Sqlite3Stmt
    groupCommit: bool
    batchState: BatchState
    readOpen: bool       ## the pass's deferred read transaction is open
    path: string         ## the database path (used once to start the worker)
    ckCtx: pointer       ## opaque CkCtx* owned by the checkpoint thread
    ckStats: CheckpointStats  ## final stats cached at stop (ckCtx is freed)

  LikeOutcome* = enum
    loLiked, loAlready, loNoPost

const
  SqlSelectPost = """
SELECT p.id, p.body, p.created_at, u.username,
       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
FROM posts p JOIN users u ON u.id = p.user_id
WHERE p.id = ?"""

  SqlSelectFeed = """
SELECT p.id, p.body, p.created_at, u.username,
       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
FROM posts p JOIN users u ON u.id = p.user_id
ORDER BY p.created_at DESC, p.id DESC
LIMIT 20"""

  # Dense-range scan. The feed is `ORDER BY created_at DESC, id DESC LIMIT 20`;
  # when those top-20 ids fit in a small dense window the ordered scan plus
  # correlated like_count subquery is replaced by three cheap range lookups
  # keyed on id. SqlSelectFeed (above) stays the decline / fallback path and is
  # used verbatim.
  #
  # 1. The top-20 ids in FEED ORDER. The slot order of this list IS the feed
  #    order, so emitting slots in order reproduces created_at DESC, id DESC.
  SqlFeedIds = """
SELECT id FROM posts
ORDER BY created_at DESC, id DESC
LIMIT 20"""

  # 2. Likes in the id window; the caller increments the slot of each post_id
  #    that belongs to the top-20 set and ignores the rest.
  SqlFeedLikes = """
SELECT post_id FROM likes
WHERE post_id BETWEEN ? AND ?"""

  # 3. Row data for the window, in feed order. Same SELECT/JOIN shape as
  #    SqlSelectFeed, minus the like_count subquery (likes come from
  #    SqlFeedLikes), keeping its ORDER BY and LIMIT. INDEXED BY pins the scan
  #    to the (created_at DESC, id DESC) index, whose order is already the feed
  #    order, so no temp B-tree sort is needed. The id-range predicate matches
  #    the bounded feed_rows statement: it excludes posts outside the
  #    top-20 id window before the users join, which measured cheaper at 64k
  #    than relying on LIMIT alone. A missing join row shifts the rows relative
  #    to the ids, which the sequential-id guard detects and declines to the
  #    ordered query.
  SqlFeedRows = """
SELECT p.id, p.body, p.created_at, u.username
FROM posts p INDEXED BY posts_created_at_id_idx
JOIN users u ON u.id = p.user_id
WHERE p.id BETWEEN ? AND ?
ORDER BY p.created_at DESC, p.id DESC
LIMIT 20"""

  SqlInsertPost = "INSERT INTO posts(user_id, body) VALUES(?, ?)"

  SqlSelectCreatedAt = "SELECT created_at FROM posts WHERE id = ?"

  SqlInsertLike = """
INSERT INTO likes(user_id, post_id)
SELECT ?, ? WHERE EXISTS(SELECT 1 FROM posts WHERE id = ?)
ON CONFLICT(user_id, post_id) DO NOTHING"""

  SqlExistsPost = "SELECT 1 FROM posts WHERE id = ?"

  SqlHealth = "SELECT 1"

  # Read/write transaction control. Prepared once and stepped, so
  # opening/closing a transaction costs a step and nothing else. The batch
  # control statements are distinct from the read ones: a COMMIT that failed
  # must be reset before its ROLLBACK, and keeping the statements separate means
  # each one has exactly one in-flight use.
  SqlBeginRead = "BEGIN DEFERRED"
  SqlCommitRead = "COMMIT"
  SqlBeginBatch = "BEGIN IMMEDIATE"
  SqlCommitBatch = "COMMIT"
  SqlRollbackBatch = "ROLLBACK"

# ---------------------------------------------------------------------------
# small helpers

proc errMsg(db: Db): string =
  if db.handle == nil: "database handle is nil"
  else: $sqlite3_errmsg(db.handle)

proc fail(db: Db, what: string) {.noreturn.} =
  raise newException(DbError,
    what & " failed (rc=" & $sqlite3_errcode(db.handle) & "): " & db.errMsg())

proc execPragma(db: Db, pragma: string, tolerate: bool) =
  ## Run a single PRAGMA. When `tolerate`, a failure (e.g. journal_mode already
  ## WAL under a second connection) is ignored instead of aborting open.
  let rc = sqlite3_exec(db.handle, ("PRAGMA " & pragma).cstring, nil, nil, nil)
  if rc != SQLITE_OK and not tolerate:
    db.fail("PRAGMA " & pragma)

proc applyPragmas(db: Db) =
  ## Applied in this order. WAL must succeed: the off-loop checkpoint design
  ## assumes a WAL database, and journal_mode=WAL is idempotent on an existing
  ## WAL database.
  ##
  ## `locking_mode=NORMAL` (not EXCLUSIVE): EXCLUSIVE is a little cheaper per
  ## statement, but it makes the file unopenable by the checkpointer's second
  ## connection. The checkpoint connection must exist from the first WAL
  ## access, so NORMAL is the mode from the start (never a mid-run switch).
  db.execPragma("journal_mode=WAL", tolerate = false)
  db.execPragma("synchronous=NORMAL", tolerate = false)
  db.execPragma("busy_timeout=5000", tolerate = false)
  db.execPragma("foreign_keys=ON", tolerate = false)
  db.execPragma("temp_store=MEMORY", tolerate = false)
  db.execPragma("cache_size=-64", tolerate = false)
  # mmap size is env-tunable (SQLITE_MMAP_SIZE, bytes; default 1 GiB).
  var mmapBytes = 1073741824
  try:
    mmapBytes = parseInt(getEnv("SQLITE_MMAP_SIZE", "1073741824"))
  except ValueError:
    mmapBytes = 1073741824
  db.execPragma("mmap_size=" & $mmapBytes, tolerate = false)
  # WAL autocheckpoint is DISABLED (0): the WAL is checkpointed explicitly by
  # the dedicated checkpoint connection/thread (SQLITE_CHECKPOINT_PASSIVE,
  # coalesced to ~1/s). That keeps the checkpoint copy + fsync off the request
  # thread. journal_size_limit is still set, but note a PASSIVE checkpoint can
  # trail the write head while the writer is continuously active, so the WAL
  # file may grow until a natural quiet point lets SQLite reset it (measured:
  # bounded at 64 MiB at 32k paced, growing during a saturated 64k run).
  # Disk headroom: under a 5-minute 64k hold the WAL reached ~143-225 MiB in
  # 60 s (~3.7 MB/s) and needs roughly 1 GiB of free disk; 32k paced settles at
  # the 64 MiB journal_size_limit. RESTART/FULL are never used under load
  # because they wait for readers and block the writer.
  db.execPragma("wal_autocheckpoint=0", tolerate = false)
  db.execPragma("journal_size_limit=67108864", tolerate = false)
  db.execPragma("locking_mode=NORMAL", tolerate = false)

proc prepare(db: Db, sql: string): ptr Sqlite3Stmt =
  var stmt: ptr Sqlite3Stmt
  let rc = sqlite3_prepare_v3(db.handle, sql.cstring, sql.len.cint,
                              SQLITE_PREPARE_PERSISTENT, addr stmt, nil)
  if rc != SQLITE_OK:
    db.fail("prepare")
  result = stmt

proc colText(stmt: ptr Sqlite3Stmt, i: cint): string =
  ## Copy a TEXT column byte-exactly; values are not guaranteed NUL-terminated,
  ## so length comes from sqlite3_column_bytes (called after column_text).
  let p = sqlite3_column_text(stmt, i)
  let n = sqlite3_column_bytes(stmt, i)
  if p == nil or n <= 0:
    return ""
  result = newString(n)
  copyMem(addr result[0], p, n)

proc readRow(stmt: ptr Sqlite3Stmt): Post =
  result.id = sqlite3_column_int64(stmt, 0)
  result.body = colText(stmt, 1)
  result.createdAt = colText(stmt, 2)
  result.author = colText(stmt, 3)
  result.likeCount = sqlite3_column_int64(stmt, 4)

proc bindInt(stmt: ptr Sqlite3Stmt, idx: cint, v: int64) =
  let rc = sqlite3_bind_int64(stmt, idx, v)
  if rc != SQLITE_OK:
    raise newException(DbError, "sqlite3_bind_int64 failed (rc=" & $rc & ")")

proc bindText(stmt: ptr Sqlite3Stmt, idx: cint, s: string) =
  let rc = sqlite3_bind_text(stmt, idx, s.cstring, s.len.cint, SQLITE_TRANSIENT)
  if rc != SQLITE_OK:
    raise newException(DbError, "sqlite3_bind_text failed (rc=" & $rc & ")")

proc resetStmt(stmt: ptr Sqlite3Stmt) =
  ## Make the statement reusable. Bindings are NOT cleared: every parameter is
  ## rebound before each execution, and the statements with no parameters never
  ## had any. `sqlite3_clear_bindings` on reset was pure per-call work (A2).
  discard sqlite3_reset(stmt)

proc stepOnce(stmt: ptr Sqlite3Stmt): cint =
  ## Step a single-shot statement (INSERT / transaction control) and reset it
  ## before returning, so no statement is left ACTIVE across the next SQL call.
  defer: resetStmt(stmt)
  result = sqlite3_step(stmt)

proc addJColText(buf: var string, stmt: ptr Sqlite3Stmt, i: cint) =
  ## Append column `i` as a JSON string (including the quotes). The column
  ## pointer is escaped in place immediately, before any further sqlite3 call,
  ## and its length comes from sqlite3_column_bytes (column text is not
  ## NUL-terminated). A NULL/empty column still emits `""`, exactly like the
  ## old colText -> Post -> object serializer path.
  buf.add '"'
  let p = sqlite3_column_text(stmt, i)
  if p != nil:
    let n = sqlite3_column_bytes(stmt, i)
    if n > 0:
      addEscapedRaw(buf, cast[ptr UncheckedArray[char]](p), int(n))
  buf.add '"'

# ---------------------------------------------------------------------------
# per-pass read transaction
#
# A deferred BEGIN takes no lock until the first read actually steps, so a pass
# that only writes pays nothing. Once open, the transaction owns the snapshot:
# resetting an individual statement does NOT end it. The server ends it at the
# pass boundary (see `runLoop`), and `beginBatch` ends it before a write.

proc beginRead(db: Db) =
  ## Open the pass's read transaction, at most once per pass and never while a
  ## write batch is in effect (invariant a).
  if db.readOpen or db.batchState != bsNone:
    return
  let stmt = db.stmtBeginRead
  defer: resetStmt(stmt)
  let rc = sqlite3_step(stmt)
  if rc != SQLITE_DONE:
    db.fail("BEGIN DEFERRED")
  db.readOpen = true

proc endRead*(db: Db): bool =
  ## End the pass's read transaction. Returns true when no read transaction is
  ## open afterwards; on a failed COMMIT the flag is kept so the next call (or
  ## the next pass boundary) retries rather than pretending the snapshot is
  ## gone. The server calls this after the write commit and before the WAL
  ## checkpoint; no read snapshot may outlive its pass (invariant b).
  if not db.readOpen:
    return true
  let stmt = db.stmtCommitRead
  defer: resetStmt(stmt)
  result = sqlite3_step(stmt) == SQLITE_DONE
  if result:
    db.readOpen = false

# ---------------------------------------------------------------------------
# group-commit batch control
#
# One batch == one explicit `BEGIN IMMEDIATE ... COMMIT` covering every write
# made during one event-loop pass. `BEGIN IMMEDIATE` (not a deferred BEGIN) takes
# the write lock up front, so a write inside the pass can never be split across
# transactions. Reads issued while the batch is open share the transaction and
# therefore observe the batch's own writes.

proc setGroupCommit*(db: Db, on: bool) =
  ## Enable/disable write batching. Called once at startup from the GROUP_COMMIT
  ## environment flag (default ON in the server). Disabling it restores the old
  ## autocommit behavior byte-for-byte.
  db.groupCommit = on

proc batchOpen*(db: Db): bool =
  ## True while a batch (open OR failed-but-not-yet-finalized) is in effect. The
  ## server uses this to decide whether a response must be withheld until commit.
  db.batchState != bsNone

proc beginBatch*(db: Db) =
  ## Called before every write. Ends the pass's read transaction first: SQLite
  ## cannot BEGIN inside an open transaction, and with group commit off an
  ## autocommit write must not silently join the read snapshot (which would
  ## defer its durability to the pass boundary). With group commit on it then
  ## opens the pass's `BEGIN IMMEDIATE`; idempotent within a pass. A failed open
  ## marks the batch failed so nothing in the pass can be reported as a
  ## success.
  if db.batchState != bsNone:
    return
  if not db.endRead():
    raise newException(DbError, "ending the pass read transaction failed")
  if not db.groupCommit:
    return
  if stepOnce(db.stmtBeginBatch) == SQLITE_DONE:
    db.batchState = bsOpen
  else:
    db.batchState = bsFailed

proc abortBatch*(db: Db) =
  ## Give up on the current batch: roll it back (releasing the write lock and
  ## discarding every uncommitted write) and remember the failure so `commitBatch`
  ## reports false. Safe to call when no batch is open.
  if db.batchState == bsOpen:
    discard stepOnce(db.stmtRollbackBatch)
  if db.batchState != bsNone:
    db.batchState = bsFailed

proc commitBatch*(db: Db): bool =
  ## End the pass's batch. Returns true only when every write in the batch is
  ## durable. On any error it ROLLBACKs and returns false, so the caller must not
  ## treat any write in the batch as committed.
  case db.batchState
  of bsNone:
    result = true
  of bsFailed:
    db.batchState = bsNone
    result = false
  of bsOpen:
    if stepOnce(db.stmtCommitBatch) == SQLITE_DONE:
      db.batchState = bsNone
      result = true
    else:
      discard stepOnce(db.stmtRollbackBatch)
      db.batchState = bsNone
      result = false

# ---------------------------------------------------------------------------
# off-loop WAL checkpoint
#
# The request connection never runs a checkpoint. A second connection, owned by
# a dedicated pthread (src/checkpoint.nim), runs PASSIVE checkpoints. The
# only coupling between the threads is SQLite's own database/WAL state plus:
#   * an atomic `checkpointWanted` flag raised by the WAL hook, and
#   * an eventfd written when the flag transitions 0 -> 1 to wake the worker.
# The hook runs on the request thread after a commit releases the write lock.
# It calls NO SQLite API: doing a checkpoint there would put the copy/fsync back
# on the request thread, which is exactly what this design removes.
#
# The second connection is READWRITE because a checkpoint must write database
# pages back into the DB file, but it never executes a data-modifying statement.
# It runs exactly one PASSIVE checkpoint per interval (coalesced); RESTART and
# FULL are deliberately never used while the writer is active because both wait
# for readers and block new writers.

proc startCheckpointer*(db: Db) =
  ## Create the checkpoint connection and its worker thread, and install the
  ## WAL hook on the request connection. Called once at startup (and by the DB
  ## unit test). Raises if the worker cannot start: shipping a server whose WAL
  ## is never checkpointed would grow it without bound.
  if db.ckCtx != nil:
    return
  db.ckCtx = ckStart(db.path.cstring, 1000)
  if db.ckCtx == nil:
    raise newException(DbError, "checkpoint thread failed to start")
  # The hook belongs to the request connection only. The checkpoint connection
  # has no hook and takes no part in signalling.
  ckInstallHook(cast[pointer](db.handle), db.ckCtx)

proc requestCheckpoint*(db: Db) =
  ## Raise the same flag the WAL hook raises. Not needed on the production path
  ## (every commit triggers the hook); exposed for tests and shutdown.
  if db.ckCtx != nil:
    discard ckSignal(db.ckCtx)

proc checkpointStats*(db: Db): CheckpointStats =
  ## Live counters while the worker runs, or the final counters cached at
  ## `stopCheckpointer`. Never reads a freed CkCtx.
  if db.ckCtx != nil:
    ckStats(db.ckCtx, addr result)
  else:
    result = db.ckStats

proc stopCheckpointer*(db: Db) =
  ## Stop and join the worker; any checkpoint still wanted runs before the
  ## thread exits, and its final counters are cached on `db`.
  ##
  ## The WAL hook is uninstalled BEFORE the context is freed: otherwise the
  ## request connection's pWalArg would still point at it and a later COMMIT
  ## would call ckSignal() on freed memory. Clearing `ckCtx` first makes a
  ## second call a no-op (single owner, idempotent).
  if db.ckCtx == nil:
    return
  let ctx = db.ckCtx
  db.ckCtx = nil
  ckUninstallHook(cast[pointer](db.handle))
  var st: CheckpointStats
  ckStop(ctx, addr st)
  db.ckStats = st

# ---------------------------------------------------------------------------
# public API

# Statement lifetime
# ---------------------------------------------------------------------------
# `sqlite3_step` returning SQLITE_ROW leaves the statement ACTIVE until it is
# reset, finalized, or stepped to completion, so every reader below resets its
# statement as soon as it has copied the rows it needs (invariant c). The reset
# keeps the statements reusable and prevents a dangling active statement; it is
# NOT what controls the snapshot any more -- the pass's explicit `BEGIN
# DEFERRED` transaction owns that snapshot until `endRead` COMMITs it.

proc openDb*(path: string): Db =
  ## Open the request-loop connection with `locking_mode=NORMAL` and
  ## `SQLITE_OPEN_NOMUTEX` (this connection is only ever used by the thread
  ## that opened it). The checkpoint connection is created later by
  ## `startCheckpointer`.
  ##
  ## The bundled SQLite is compiled with SQLITE_OMIT_AUTOINIT (see sqlite3.nim),
  ## so the library is initialized exactly once, here, before the first call into
  ## it. sqlite3_initialize() is idempotent and is the ONLY SQLite entry point
  ## that may run before this call returns. Threading mode is verified after
  ## initialization: a second connection on another thread is only legal when
  ## the amalgamation was compiled SQLITE_THREADSAFE=2.
  let irc = sqlite3_initialize()
  if irc != SQLITE_OK:
    raise newException(DbError, "sqlite3_initialize failed (rc=" & $irc & ")")
  if sqlite3_threadsafe() != 2:
    raise newException(DbError,
      "SQLite is not compiled SQLITE_THREADSAFE=2 (sqlite3_threadsafe()=" &
      $(sqlite3_threadsafe()) & "); the checkpoint connection is unsafe")

  new(result)
  result.path = path
  let rc = sqlite3_open_v2(path.cstring, addr result.handle,
                           SQLITE_OPEN_READWRITE or SQLITE_OPEN_NOMUTEX, nil)
  if rc != SQLITE_OK:
    let msg = result.errMsg()
    if result.handle != nil:
      discard sqlite3_close_v2(result.handle)
    raise newException(DbError, "sqlite3_open_v2 failed (rc=" & $rc & "): " & msg)

  result.applyPragmas()

  result.stmtGetPost = result.prepare(SqlSelectPost)
  result.stmtFeed = result.prepare(SqlSelectFeed)
  result.stmtFeedIds = result.prepare(SqlFeedIds)
  result.stmtFeedLikes = result.prepare(SqlFeedLikes)
  result.stmtFeedRows = result.prepare(SqlFeedRows)
  result.stmtInsertPost = result.prepare(SqlInsertPost)
  result.stmtCreatedAt = result.prepare(SqlSelectCreatedAt)
  result.stmtInsertLike = result.prepare(SqlInsertLike)
  result.stmtExistsPost = result.prepare(SqlExistsPost)
  result.stmtHealth = result.prepare(SqlHealth)
  result.stmtBeginRead = result.prepare(SqlBeginRead)
  result.stmtCommitRead = result.prepare(SqlCommitRead)
  result.stmtBeginBatch = result.prepare(SqlBeginBatch)
  result.stmtCommitBatch = result.prepare(SqlCommitBatch)
  result.stmtRollbackBatch = result.prepare(SqlRollbackBatch)

proc health*(db: Db): bool =
  ## The health probe needs no snapshot: `SELECT 1` reads no table, so it does
  ## not open the pass's read transaction (and a health-only pass pays no
  ## BEGIN/COMMIT). The pass-end `endRead` no-ops when no transaction is open.
  let stmt = db.stmtHealth
  defer: resetStmt(stmt)
  result = sqlite3_step(stmt) == SQLITE_ROW

proc getPost*(db: Db, id: int64, post: var Post): bool =
  db.beginRead()
  let stmt = db.stmtGetPost
  defer: resetStmt(stmt)      # also on db.fail: never leave it ACTIVE
  bindInt(stmt, 1, id)
  let rc = sqlite3_step(stmt)
  if rc == SQLITE_ROW:
    post = readRow(stmt)
    result = true
  elif rc == SQLITE_DONE:
    result = false
  else:
    db.fail("getPost step")

proc writePostRow(buf: var string, stmt: ptr Sqlite3Stmt, likeCount: int64) =
  ## Emit one feed row as a JSON object straight from the statement's columns.
  ## Columns 0..3 are id/body/created_at/author (the SELECT shape shared by
  ## SqlSelectFeed and SqlFeedRows); like_count is supplied because the dense
  ## scan counts likes separately. Keys, order and escaping match
  ## the object serializer exactly.
  buf.add "{\"id\":"
  buf.addJInt sqlite3_column_int64(stmt, 0)
  buf.add ",\"body\":"
  addJColText(buf, stmt, 1)
  buf.add ",\"created_at\":"
  addJColText(buf, stmt, 2)
  buf.add ",\"author\":"
  addJColText(buf, stmt, 3)
  buf.add ",\"like_count\":"
  buf.addJInt likeCount
  buf.add "}"

proc writePostJson*(db: Db, id: int64, buf: var string): bool =
  ## Write the GET /posts/:id response body (`{"post":...}`) straight from the
  ## statement's columns, exactly like the feed row writer. This replaces the
  ## object path (readRow -> Post -> object serializer) on the request hot path: no Post,
  ## no three column strings and no second escaping pass. `buf` is untouched
  ## unless the post exists, so the caller's error response cannot inherit a
  ## partial body. Column 4 is the correlated like_count from SqlSelectPost.
  db.beginRead()
  let stmt = db.stmtGetPost
  defer: resetStmt(stmt)      # also on db.fail: never leave it ACTIVE
  bindInt(stmt, 1, id)
  let rc = sqlite3_step(stmt)
  if rc == SQLITE_ROW:
    buf.add "{\"post\":"
    writePostRow(buf, stmt, sqlite3_column_int64(stmt, 4))
    buf.add "}"
    result = true
  elif rc == SQLITE_DONE:
    result = false
  else:
    db.fail("getPost step")

proc writeFeedFallbackJson*(db: Db, buf: var string) =
  ## The original feed query (ORDER BY created_at DESC, id DESC LIMIT 20 with
  ## the correlated like_count subquery), serialized directly as the decline
  ## path for non-dense id windows. `buf` is truncated by the caller on decline,
  ## so this always starts at the response body boundary. It joins the pass's
  ## read transaction like every other reader.
  db.beginRead()
  let stmt = db.stmtFeed
  defer: resetStmt(stmt)      # also on db.fail: never leave it ACTIVE
  buf.add "{\"posts\":["
  var first = true
  while true:
    let rc = sqlite3_step(stmt)
    if rc == SQLITE_ROW:
      if first:
        first = false
      else:
        buf.add ','
      writePostRow(buf, stmt, sqlite3_column_int64(stmt, 4))
    elif rc == SQLITE_DONE:
      break
    else:
      db.fail("feed step")
  buf.add "]}"

proc writeFeedJson*(db: Db, buf: var string) =
  ## Serialize the feed (created_at DESC, id DESC, up to 20 posts) into `buf`
  ## straight from SQLite's columns. No Post object, per-column string or
  ## intermediate body string is built; the bytes match the previous
  ## Post-object serialization.
  ##
  ## Dense-range scan: the top-20 ids usually fit in a
  ## small window, so three cheap range statements (ids, likes, rows) replace
  ## the ordered query with its 20 correlated like_count subqueries. The row
  ## scan is bounded by `WHERE p.id BETWEEN ? AND ?` over that same id window.
  ## Rows are ordered exactly like the ids, so each row is written as it is
  ## stepped; the slot map only attributes the like counts gathered from the id
  ## window.
  ##
  ## Declines to the original ordered query when:
  ##   * (maxId - minId) >= 256 : the window would scan too many unrelated rows.
  ##   * a row is missing or out of order : the scan is not a faithful
  ##     replacement (e.g. a top id whose join row is absent).
  const MaxSpan = 256
  let mark = buf.len            # body boundary; restored on decline
  db.beginRead()

  # 1. top-20 ids, in feed order.
  let idsStmt = db.stmtFeedIds
  defer: resetStmt(idsStmt)   # also on db.fail: never leave it ACTIVE
  var topIds: array[20, int64]
  var n = 0
  while true:
    let rc = sqlite3_step(idsStmt)
    if rc == SQLITE_ROW:
      topIds[n] = sqlite3_column_int64(idsStmt, 0)
      inc n
    elif rc == SQLITE_DONE:
      break
    else:
      db.fail("feed ids step")

  if n == 0:
    buf.add "{\"posts\":[]}"
    return

  # 2. window bounds and density gate.
  var minId = topIds[0]
  var maxId = topIds[0]
  for i in 1 ..< n:
    if topIds[i] < minId: minId = topIds[i]
    if topIds[i] > maxId: maxId = topIds[i]
  if maxId - minId >= MaxSpan:
    buf.setLen(mark)              # defensive: no dense bytes may leak
    db.writeFeedFallbackJson(buf)
    return

  # 3. slot map: id offset in the window -> feed position (0..n-1).
  var slotOf: array[MaxSpan, int]
  for i in 0 ..< MaxSpan: slotOf[i] = -1
  var likeFor: array[20, int64]
  for i in 0 ..< n:
    slotOf[int(topIds[i] - minId)] = i
    likeFor[i] = 0

  # 4. likes in the window; count only ids that own a slot.
  let lkStmt = db.stmtFeedLikes
  defer: resetStmt(lkStmt)    # also on db.fail: never leave it ACTIVE
  bindInt(lkStmt, 1, minId)
  bindInt(lkStmt, 2, maxId)
  while true:
    let rc = sqlite3_step(lkStmt)
    if rc == SQLITE_ROW:
      let pid = sqlite3_column_int64(lkStmt, 0)
      let slot = slotOf[int(pid - minId)]
      if slot >= 0:
        inc likeFor[slot]
    elif rc == SQLITE_DONE:
      break
    else:
      db.fail("feed likes step")

  # 5. rows in the window, already in feed order; write each one immediately.
  #    minId/maxId bound the scan to the same top-20 id window (`WHERE p.id
  #    BETWEEN ? AND ?`); the index order is the feed order, and the guard below
  #    declines whenever the joined rows do not match the top-20 ids.
  let rwStmt = db.stmtFeedRows
  defer: resetStmt(rwStmt)    # also on db.fail: never leave it ACTIVE
  bindInt(rwStmt, 1, minId)
  bindInt(rwStmt, 2, maxId)
  buf.add "{\"posts\":["
  var idx = 0
  var ok = true
  while true:
    let rc = sqlite3_step(rwStmt)
    if rc == SQLITE_ROW:
      let pid = sqlite3_column_int64(rwStmt, 0)
      if idx >= n or pid != topIds[idx]:
        ok = false                  # missing/out-of-order row: use the fallback
        break
      if idx > 0: buf.add ','
      writePostRow(buf, rwStmt, likeFor[idx])
      inc idx
    elif rc == SQLITE_DONE:
      break
    else:
      db.fail("feed rows step")

  if not ok or idx != n:
    buf.setLen(mark)
    db.writeFeedFallbackJson(buf)
    return
  buf.add "]}"

# ---------------------------------------------------------------------------
# Direct feed response (no intermediate body string)
# ---------------------------------------------------------------------------

const
  ## Head room reserved ahead of the feed body. `FeedHeadFixedBytes` covers every
  ## byte except the Content-Length digits; a body length is an `int`, so at most
  ## 19 decimal digits, making `FeedHeadMaxBytes` the largest head this writer
  ## can emit (93 + 19 = 112 B). The static assertion turns the reserve into a
  ## compile-time bound instead of an unreachable runtime fallback path.
  FeedHeadFixedBytes = 93
  FeedHeadMaxDigits = 19
  FeedHeadReserve = 128
  FeedHeadMaxBytes = FeedHeadFixedBytes + FeedHeadMaxDigits
static:
  doAssert FeedHeadMaxBytes <= FeedHeadReserve,
    "FeedHeadReserve must cover the largest feed head"

proc formatFeedHead(head: var array[FeedHeadReserve, char];
                    bodyLen: int; keepAlive: bool): int =
  ## Format exactly the bytes `addResponse(200, body, keepAlive)` prepends,
  ## straight into the stack array `head`; returns the required length. No Nim
  ## string is allocated and nothing is written past `head.len`. The static
  ## bound above proves the returned length always fits.
  var n = 0
  template put(lit: string) =
    if n + lit.len <= head.len:
      for c in lit:
        head[n] = c
        inc n
    else:
      n += lit.len
  template putInt(v0: int) =
    var v = v0
    var digits: array[20, char]
    var nd = 0
    if v == 0:
      if n < head.len: head[n] = '0'
      inc n
    else:
      while v > 0:
        digits[nd] = char(ord('0') + (v mod 10))
        inc nd
        v = v div 10
      while nd > 0:
        dec nd
        if n < head.len: head[n] = digits[nd]
        inc n
  let conn = if keepAlive: "keep-alive" else: "close"
  put "HTTP/1.1 "
  put "200 "                          # the only status the direct writer emits
  put reasonPhrase(200)
  put "\r\nContent-Type: application/json\r\nContent-Length: "
  putInt bodyLen
  put "\r\nConnection: "
  put conn
  put "\r\n\r\n"
  n

proc writeFeedResponse*(db: Db, buf: var string; keepAlive: bool) =
  ## Append the complete GET /feed response - head and body - to `buf` without
  ## ever building the body (or the head) as an intermediate Nim string.
  ##
  ## The body is written by `writeFeedJson` at `start + FeedHeadReserve`; the
  ## formatted head is then moved into place over the unused part of the
  ## reserve (`moveMem`, which handles the overlapping left shift) and only the
  ## final head+body length is kept. The emitted bytes are identical to
  ## `buf.addResponse(200, body, keepAlive)` with `body` from `writeFeedJson`.
  let start = buf.len
  buf.setLen(start + FeedHeadReserve)
  db.writeFeedJson(buf)
  let bodyStart = start + FeedHeadReserve
  let bodyLen = buf.len - bodyStart

  var head: array[FeedHeadReserve, char]
  let headLen = formatFeedHead(head, bodyLen, keepAlive)

  if headLen < FeedHeadReserve and bodyLen > 0:
    moveMem(addr buf[start + headLen], addr buf[bodyStart], bodyLen)
  buf.setLen(start + headLen + bodyLen)
  if headLen > 0:
    copyMem(addr buf[start], addr head[0], headLen)

type InsertResult = object
  ok: bool
  newId: int64
  createdAt: string

proc insertPostRow(db: Db, userId: int64, body: string): InsertResult =
  ## Common INSERT + created_at re-read for both createPost variants. The insert
  ## is a single-shot statement: `stepOnce` resets it before any error handling,
  ## so a failed insert is never left ACTIVE and cannot block the batch ROLLBACK.
  db.beginBatch()
  # A batch that already failed must not run further writes: answering the
  # request 500 is correct, touching the database is not.
  if db.batchState == bsFailed:
    return
  let ins = db.stmtInsertPost
  bindInt(ins, 1, userId)
  bindText(ins, 2, body)
  let rc = stepOnce(ins)
  if rc != SQLITE_DONE:
    # A failed insert is not a success. Only surface it as false, per the frozen
    # bool contract. Under group commit this is an unexpected error, so abort
    # the whole batch.
    if db.groupCommit:
      db.abortBatch()
    return

  result.newId = sqlite3_last_insert_rowid(db.handle)

  let ca = db.stmtCreatedAt
  defer: resetStmt(ca)        # also on db.fail: never leave it ACTIVE
  bindInt(ca, 1, result.newId)
  let crc = sqlite3_step(ca)
  if crc != SQLITE_ROW:
    db.fail("createPost re-read created_at")
  # Copy before the deferred reset invalidates the column pointer.
  result.createdAt = colText(ca, 0)
  result.ok = true

proc createPost*(db: Db, userId: int64, username, body: string,
                 post: var Post): bool =
  ## INSERT. Without group commit this is autocommit (durable before returning).
  ## With group commit the row joins the pass's transaction and is durable only
  ## after the server COMMITs; the server withholds the response until then.
  let r = db.insertPostRow(userId, body)
  if not r.ok:
    return false
  post.id = r.newId
  post.body = body
  post.createdAt = r.createdAt
  post.author = username
  post.likeCount = 0
  result = true

proc createPostResponseBody*(db: Db, userId: int64, username, body: string,
                             buf: var string): bool =
  ## The POST /posts response body (`{"post":...}`) written directly from the
  ## inserted row's values, with no Post object and no re-escaping pass. The
  ## bytes are identical to the object-based create-post response. `buf` is only
  ## touched after a successful insert + created_at read, so a 500 response
  ## cannot inherit partial body bytes.
  let r = db.insertPostRow(userId, body)
  if not r.ok:
    return false
  buf.add "{\"post\":{\"id\":"
  buf.addJInt r.newId
  buf.add ",\"body\":"
  buf.addJStr body
  buf.add ",\"created_at\":"
  buf.addJStr r.createdAt
  buf.add ",\"author\":"
  buf.addJStr username
  buf.add ",\"like_count\":"
  buf.addJInt 0
  buf.add "}}"
  result = true

proc like*(db: Db, userId, postId: int64): LikeOutcome =
  ## INSERT ... ON CONFLICT(user_id, post_id) DO NOTHING. Without group commit
  ## this is autocommit (durable before returning). With group commit the like
  ## joins the pass's transaction. The statement inserts at most one row and can never hit a
  ## constraint (the post is checked with WHERE EXISTS first, and the unique
  ## (user_id, post_id) conflict is swallowed by the targeted ON CONFLICT clause
  ## (40 VM steps versus 43 for the un-targeted form), so it cannot tear the
  ## batch.
  db.beginBatch()
  if db.batchState == bsFailed:
    # The batch is already lost; do not execute. Raising maps to a 500.
    raise newException(DbError, "group-commit batch already failed")
  let ins = db.stmtInsertLike
  bindInt(ins, 1, userId)
  bindInt(ins, 2, postId)
  bindInt(ins, 3, postId)
  let rc = stepOnce(ins)
  if rc != SQLITE_DONE:
    # stepOnce already reset the statement, so the batch ROLLBACK is not
    # blocked by an ACTIVE insert.
    if db.groupCommit:
      db.abortBatch()
    db.fail("like insert")

  if sqlite3_changes(db.handle) == 1:
      return loLiked

  let ex = db.stmtExistsPost
  defer: resetStmt(ex)        # also on db.fail: never leave it ACTIVE
  bindInt(ex, 1, postId)
  let erc = sqlite3_step(ex)
  if erc == SQLITE_ROW:
    return loAlready
  elif erc == SQLITE_DONE:
    return loNoPost
  else:
    db.fail("like existence check")
