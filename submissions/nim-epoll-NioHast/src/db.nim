## Data-access layer. Owns every SQL statement in the submission.
##
## Uses the raw bindings from `sqlite3` directly (no std/db_sqlite, no ORM).
## Statements are prepared once at open time with SQLITE_PREPARE_PERSISTENT and
## reused; each call resets and re-binds.
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
## ON CONFLICT DO NOTHING` (a missing post inserts zero rows, a duplicate inserts
## zero rows) and `insert_post` has no unique constraint to violate. Any OTHER
## (unexpected) error aborts the whole batch via `abortBatch`/`commitBatch`
## rollback, and the server turns every response produced against that batch into
## a 500 -- it can never answer success for a write that did not commit.

import ./sqlite3
import ./model

type
  DbError* = object of CatchableError

  BatchState = enum
    bsNone    ## no transaction open (autocommit)
    bsOpen    ## BEGIN IMMEDIATE issued, not yet committed
    bsFailed  ## the batch hit an unexpected error; COMMIT must report failure

  Db* = ref object
    handle: ptr Sqlite3
    stmtGetPost: ptr Sqlite3Stmt
    stmtFeed: ptr Sqlite3Stmt
    stmtInsertPost: ptr Sqlite3Stmt
    stmtCreatedAt: ptr Sqlite3Stmt
    stmtInsertLike: ptr Sqlite3Stmt
    stmtExistsPost: ptr Sqlite3Stmt
    stmtHealth: ptr Sqlite3Stmt
    groupCommit: bool
    batchState: BatchState

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

  SqlInsertPost = "INSERT INTO posts(user_id, body) VALUES(?, ?)"

  SqlSelectCreatedAt = "SELECT created_at FROM posts WHERE id = ?"

  SqlInsertLike = """
INSERT INTO likes(user_id, post_id)
SELECT ?, ? WHERE EXISTS(SELECT 1 FROM posts WHERE id = ?)
ON CONFLICT DO NOTHING"""

  SqlExistsPost = "SELECT 1 FROM posts WHERE id = ?"

  SqlHealth = "SELECT 1"

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
  ## Applied in the order required by the plan; journal_mode is tolerated
  ## because it can fail when another connection already owns the WAL.
  ##
  ## `locking_mode=EXCLUSIVE` is the fastest path: no per-statement lock traffic
  ## once the lock is held.
  db.execPragma("journal_mode=WAL", tolerate = true)
  db.execPragma("synchronous=NORMAL", tolerate = false)
  db.execPragma("busy_timeout=5000", tolerate = false)
  db.execPragma("foreign_keys=ON", tolerate = false)
  db.execPragma("temp_store=MEMORY", tolerate = false)
  db.execPragma("cache_size=-8000", tolerate = false)
  db.execPragma("mmap_size=1073741824", tolerate = false)
  # Checkpoint the WAL every ~1000 pages (SQLite's default) instead of never.
  # The previous value (0) disabled autocheckpoint entirely and nothing else
  # called sqlite3_wal_checkpoint, so under sustained writes the -wal file grew
  # without bound (each write appends a frame). 1000 pages keeps it bounded;
  # journal_size_limit below then truncates the WAL at checkpoint.
  db.execPragma("wal_autocheckpoint=1000", tolerate = false)
  db.execPragma("journal_size_limit=67108864", tolerate = false)
  db.execPragma("locking_mode=EXCLUSIVE", tolerate = false)

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
  discard sqlite3_reset(stmt)
  discard sqlite3_clear_bindings(stmt)

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
  ## Start the pass's transaction on the first write. Idempotent: a second write
  ## in the same pass is a no-op. A failed open marks the batch failed so nothing
  ## in the pass can be reported as a success.
  if db.batchState != bsNone or not db.groupCommit:
    return
  let rc = sqlite3_exec(db.handle, "BEGIN IMMEDIATE".cstring, nil, nil, nil)
  if rc == SQLITE_OK:
    db.batchState = bsOpen
  else:
    db.batchState = bsFailed

proc abortBatch*(db: Db) =
  ## Give up on the current batch: roll it back (releasing the write lock and
  ## discarding every uncommitted write) and remember the failure so `commitBatch`
  ## reports false. Safe to call when no batch is open.
  if db.batchState == bsOpen:
    discard sqlite3_exec(db.handle, "ROLLBACK".cstring, nil, nil, nil)
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
    let rc = sqlite3_exec(db.handle, "COMMIT".cstring, nil, nil, nil)
    if rc == SQLITE_OK:
      db.batchState = bsNone
      result = true
    else:
      discard sqlite3_exec(db.handle, "ROLLBACK".cstring, nil, nil, nil)
      db.batchState = bsNone
      result = false

# ---------------------------------------------------------------------------
# public API

# Statement lifetime (why single-row reads are reset immediately)
# ---------------------------------------------------------------------------
# `sqlite3_step` returning SQLITE_ROW leaves the statement ACTIVE, and an active
# statement keeps the connection's implicit read transaction (and therefore its
# snapshot) open until the statement is reset, finalized, or stepped to
# completion. Single-row readers below therefore reset the statement as soon as
# they have copied the row they need, so the snapshot is released at end-of-use
# instead of lingering until the next statement is stepped.

proc openDb*(path: string): Db =
  ## Open the shared database file, always with `locking_mode=EXCLUSIVE`.
  ##
  ## The bundled SQLite is compiled with SQLITE_OMIT_AUTOINIT (see sqlite3.nim),
  ## so the library is initialized exactly once, here, before the first call into
  ## it. sqlite3_initialize() is idempotent and is the ONLY SQLite entry point
  ## that may run before this call returns.
  let irc = sqlite3_initialize()
  if irc != SQLITE_OK:
    raise newException(DbError, "sqlite3_initialize failed (rc=" & $irc & ")")

  new(result)
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
  result.stmtInsertPost = result.prepare(SqlInsertPost)
  result.stmtCreatedAt = result.prepare(SqlSelectCreatedAt)
  result.stmtInsertLike = result.prepare(SqlInsertLike)
  result.stmtExistsPost = result.prepare(SqlExistsPost)
  result.stmtHealth = result.prepare(SqlHealth)

proc health*(db: Db): bool =
  let stmt = db.stmtHealth
  resetStmt(stmt)
  result = sqlite3_step(stmt) == SQLITE_ROW
  # Release the implicit read transaction now that we have the row. See the
  # note on statement lifetime above `openDb`.
  resetStmt(stmt)

proc getPost*(db: Db, id: int64, post: var Post): bool =
  let stmt = db.stmtGetPost
  resetStmt(stmt)
  bindInt(stmt, 1, id)
  let rc = sqlite3_step(stmt)
  if rc == SQLITE_ROW:
    post = readRow(stmt)
    result = true
  elif rc == SQLITE_DONE:
    result = false
  else:
    db.fail("getPost step")
  resetStmt(stmt)

proc feed*(db: Db, posts: var seq[Post]) =
  posts.setLen(0)
  let stmt = db.stmtFeed
  resetStmt(stmt)
  while true:
    let rc = sqlite3_step(stmt)
    if rc == SQLITE_ROW:
      posts.add readRow(stmt)
    elif rc == SQLITE_DONE:
      break
    else:
      db.fail("feed step")

proc createPost*(db: Db, userId: int64, username, body: string,
                 post: var Post): bool =
  ## INSERT. Without group commit this is autocommit (durable before returning).
  ## With group commit the row joins the pass's transaction and is durable only
  ## after the server COMMITs; the server withholds the response until then.
  if db.groupCommit:
    db.beginBatch()
    # A batch that already failed must not run further writes: answering the
    # request 500 is correct, touching the database is not.
    if db.batchState == bsFailed:
      return false
  let ins = db.stmtInsertPost
  resetStmt(ins)
  bindInt(ins, 1, userId)
  bindText(ins, 2, body)
  let rc = sqlite3_step(ins)
  if rc != SQLITE_DONE:
    # Statement finished (no reset-with-error); a failed insert is not a
    # success. Only surface it as false, per the frozen bool contract. Under
    # group commit this is an unexpected error, so abort the whole batch.
    if db.groupCommit:
      db.abortBatch()
    return false

  let newId = sqlite3_last_insert_rowid(db.handle)

  let ca = db.stmtCreatedAt
  resetStmt(ca)
  bindInt(ca, 1, newId)
  var createdAt = ""
  let crc = sqlite3_step(ca)
  if crc != SQLITE_ROW:
    db.fail("createPost re-read created_at")
  createdAt = colText(ca, 0)
  resetStmt(ca)          # release the read snapshot (see the note above openDb)

  post.id = newId
  post.body = body
  post.createdAt = createdAt
  post.author = username
  post.likeCount = 0
  result = true

proc like*(db: Db, userId, postId: int64): LikeOutcome =
  ## INSERT ... ON CONFLICT DO NOTHING. Without group commit this is autocommit
  ## (durable before returning). With group commit the like joins the pass's
  ## transaction. The statement inserts at most one row and can never hit a
  ## constraint (the post is checked with WHERE EXISTS first, and the unique
  ## (user_id, post_id) conflict is swallowed by ON CONFLICT DO NOTHING), so it
  ## cannot tear the batch.
  if db.groupCommit:
    db.beginBatch()
    if db.batchState == bsFailed:
      # The batch is already lost; do not execute. Raising maps to a 500.
      raise newException(DbError, "group-commit batch already failed")
  let ins = db.stmtInsertLike
  resetStmt(ins)
  bindInt(ins, 1, userId)
  bindInt(ins, 2, postId)
  bindInt(ins, 3, postId)
  let rc = sqlite3_step(ins)
  if rc != SQLITE_DONE:
    if db.groupCommit:
      db.abortBatch()
    db.fail("like insert")

  if sqlite3_changes(db.handle) == 1:
    return loLiked

  let ex = db.stmtExistsPost
  resetStmt(ex)
  bindInt(ex, 1, postId)
  let erc = sqlite3_step(ex)
  if erc == SQLITE_ROW:
    resetStmt(ex)        # release the read snapshot (see the note above openDb)
    return loAlready
  elif erc == SQLITE_DONE:
    return loNoPost
  else:
    db.fail("like existence check")
