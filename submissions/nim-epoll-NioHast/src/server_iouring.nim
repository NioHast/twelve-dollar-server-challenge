## server_iouring.nim: the DEFAULT io_uring transport.
##
## Reuses the frozen `api.nim`/`db.nim`/`config.nim` modules unchanged, the raw
## io_uring bindings in `iouring.nim`, and the shared HTTP parser/framing in
## `http.nim`; it does NOT import `server.nim` (that file is a program, not a
## module). The epoll transport `src/server.nim` is the always-built fallback:
## `start.sh` prefers this binary and drops back to `bin/server`, and this binary
## itself execs `bin/server` when `io_uring_setup` fails at run time.
##
## Why io_uring here
## -----------------
## The one thing this transport does that epoll cannot is *registered
## (fixed) file descriptors*. Accepted connections live in the ring's
## fixed-file table and every subsequent RECV/SEND addresses them by slot index
## with `IOSQE_FIXED_FILE`. The fixed table holds its own reference, so a
## connection no longer consumes a process `RLIMIT_NOFILE` slot. The registered
## table is what the famous 300k-connection C entry rides; here it is strictly
## OPT-IN and the default path is byte-for-byte the old epoll server.
##
## Two io_uring best-practices are layered on top of the basic transport:
##
##  1. **Ring setup flags** (`IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN`, plus
##     `COOP_TASKRUN`): the recommended set for a single-threaded, per-thread
##     ring.  Less cross-CPU locking, and completion (task) work is deferred to
##     `io_uring_enter`, which the loop already calls every pass.  If the kernel
##     rejects the flags with `-EINVAL` the ring is re-created without them and
##     startup proceeds; the mode is logged.
##
##  2. **Direct multishot accept** (`IORING_ACCEPT_MULTISHOT` with the SQE's
##     `file_index = IORING_FILE_INDEX_ALLOC`): the kernel installs each accepted
##     socket straight into the first free fixed-file slot, so no
##     `IORING_REGISTER_FILES_UPDATE` and no per-accept fd allocation are needed.
##     The accept CQE's `res` is the slot index.  If the kernel rejects the
##     direct variant (`-EINVAL`/`-EOPNOTSUPP`), we transparently fall back to
##     the register-per-accept path.  Closing a connection still frees its slot
##     with `FILES_UPDATE(-1)`, so the slot is reused by the next direct accept.
##
## Fidelity
## --------
## The HTTP/1.1 parser, response framing, keep-alive, pipelining,
## `Content-Length` handling and malformed-framing policy live in the shared
## `http.nim`, so the bytes this transport emits are identical to the epoll
## server's by construction. The socket is still single-threaded; the event
## source is the io_uring completion queue instead of `epoll_wait`.

import std/[posix, os, strutils, times, nativesockets]

import iouring, config, db, api, http

# ---------------------------------------------------------------------------
# syscall surface std/posix and iouring.nim do not expose for us
# (raw `syscall`, never liburing)

proc c_syscall(number: clong): clong
  {.importc: "syscall", header: "<unistd.h>", varargs.}
var SYS_io_uring_register {.importc, header: "<sys/syscall.h>".}: clong

var SOCK_NONBLOCK {.importc, header: "<sys/socket.h>".}: cint
var SOCK_CLOEXEC  {.importc, header: "<sys/socket.h>".}: cint

# NOTE: we deliberately do NOT `importc` errno from <errno.h> here. Pulling
# <errno.h> into the translation unit macro-expands the `errno` field of
# `IoRingError` (iouring.nim) and the generated struct fails to compile.
# iouring.nim itself reports syscall errors via `osLastError()`, so we do the same.
proc lastErrno(): cint {.inline.} = cint(osLastError().int)

# ---------------------------------------------------------------------------
# io_uring constants not already exported by iouring.nim (mirrored from
# <linux/io_uring.h>)

const
  IOSQE_FIXED_FILE*        = 1'u8 shl 0

  IORING_OP_TIMEOUT*       = 11'u8
  IORING_OP_ASYNC_CANCEL*  = 14'u8

  IORING_REGISTER_FILES_UPDATE* = 6
  IORING_ASYNC_CANCEL_USERDATA  = 1'u32 shl 4

  # completion flags
  IORING_CQE_F_MORE*       = 1'u32 shl 1

  # user_data operation tags (low 8 bits) + slot (next 24) + generation (high 32)
  TAG_ACCEPT*  = 1'u64
  TAG_RECV*    = 2'u64
  TAG_SEND*    = 3'u64
  TAG_TIMEOUT* = 4'u64
  TAG_CANCEL*  = 5'u64
  TAG_PROVIDE* = 6'u64

  # ring / table sizing.
  # FixedSlots is the registered-file table size. Registering 65536 slots fails
  # outright when RLIMIT_NOFILE is 65535 (`IORING_REGISTER_FILES` needs a slot
  # per entry), so keep it comfortably below that ceiling; main() also raises the
  # soft fd limit first.
  RingEntries  = 4096
  FixedSlots   = 32768
  ListenSlot   = 0'i32

  # read/parse limits (identical policy to server.nim)
  MaxInBuf      = 1024 * 1024
  MaxRequest    = MaxInBuf + MaxHeader + 4
  # Output cap (mirror server.nim): processing stops once this many response
  # bytes are buffered, which leaves output in flight so the drain path resumes
  # the remaining pipelined requests.
  MaxOutBuf     = 2 * 1024 * 1024
  ListenBacklog = 4096
  IdleKeepAlive = 75.0
  StallTimeout  = 30.0
  TickSeconds   = 1.0

  # -------------------------------------------------------------------------
  # Provided-buffer pool ("the turbo").
  #
  # Multishot recv selects its landing buffer from a single SHARED pool instead
  # of every connection owning a private 16 KiB recv buffer. Idle connections
  # therefore hold no recv storage at all (a buffer is taken only while bytes
  # are actually in flight), which is what removes the per-connection RSS that
  # the one-shot design paid at 10k connections.
  #
  # The pool is one lazily-faulted allocation: `alloc` gives untouched pages, so
  # only the buffers the kernel has actually written into become resident. The
  # descriptors live in a page-aligned ring registered once with
  # IORING_REGISTER_PBUF_RING; when the kernel cannot do that we fall back to
  # IORING_OP_PROVIDE_BUFFERS over the exact same buffer memory.
  BufGroup       = 0'u16
  BufSize        = 8 * 1024
  # Default pool = 16 MiB resident at 10k VUs and ~equal to epoll at 20k, with
  # no p99 cost; IOURING_BUFS can raise it up to BufRingMax. Both values MUST be
  # powers of two (buf-ring requirement).
  BufRingDefault = 2048
  BufRingMax     = 8192

# kernel-side struct for IORING_OP_TIMEOUT
type KernelTimespec {.bycopy.} = object
  tv_sec:  int64
  tv_nsec: int64

# ---------------------------------------------------------------------------
# connection state: one slot per entry in the registered file table

type
  Conn = object
    active:     bool
    slot:       cint
    gen:        uint32   ## bumped per reuse so stale CQEs are ignored
    inbuf:      string
    outbuf:     string
    outOff:     int
    keepAlive:  bool
    closing:    bool
    readPaused: bool     ## stopped reading at the input cap with data pending
    recvArmed:  bool
    sendArmed:  bool
    wantFree:   bool     ## slot should return to the free list once pending==0
    pending:    int      ## in-flight io_uring ops referencing our buffers
    lastActive: float
    recvStarved: bool    ## recv ended with -ENOBUFS; re-arm when a buffer frees
    deferred:   bool     ## send withheld until the pass's group commit finishes
    batchFrom:  int      ## outbuf offset where this pass's batch output begins
    batchCount: int      ## responses appended while the pass's batch was open

var
  gRing:        Ring
  gDb:          Db
  gSecret:      string
  conns:        seq[Conn]      # indexed by fixed-file slot, never resized
  gFreeSlots:   seq[cint]
  gFreeTop:     int
  gGen:         uint32
  gNow:         float
  gAcceptArmed: bool
  gAcceptPaused: bool
  gTimeoutArmed: bool
  gListenRawFd: cint = -1
  openCount:    int

  # ---- turbo toggles (env) -------------------------------------------------
  # IOURING_RECV=multishot|oneshot (default multishot) and
  # IOURING_ACCEPT=multishot|oneshot (default multishot). Both fall back at runtime
  # if the kernel refuses the feature, and `-ENOBUFS`/`-EINVAL` are always
  # handled, so a one-shot build is a single env var away if p99 regresses.
  gRecvMultishot:   bool = true
  gAcceptMultishot: bool = true
  gAcceptMsProven:  bool = false
  # Direct accept: install the accepted socket straight into a fixed slot with
  # IORING_FILE_INDEX_ALLOC (kernel picks the slot; CQE res == slot). IOURING_ACCEPT_DIRECT=0
  # forces the register-per-accept fallback; it also engages automatically if the
  # kernel rejects the direct variant. Only meaningful with multishot accept.
  gAcceptDirect:       bool = true
  gAcceptDirectProven: bool = false

  # ---- provided-buffer pool ------------------------------------------------
  gBufBase:     ptr UncheckedArray[char]   # gBufSlots * BufSize, lazily faulted
  gBufRing:     ptr UncheckedArray[BufRingEntry]  # page-aligned descriptor ring
  gBufTail:     uint16
  gBufSlots:    int = BufRingDefault       # live pool size (IOURING_BUFS, power of 2)
  gBufMask:     int = BufRingDefault - 1
  gUseBufRing:  bool                       # true: registered PBUF_RING; else list
  gBufsReady:   int                        # buffers handed to the kernel, not yet seen
  gConsumedThisPass: int                   # data CQEs that returned a buffer this pass
  gRecvStarved: seq[cint]                  # slots whose recv ended with -ENOBUFS
  gSendRetry:   seq[cint]                  # slots whose send ended with -ENOBUFS

  # Slots whose SEND was withheld because their responses were produced while a
  # group-commit batch was open. Armed (or rewritten to 500) once the batch has
  # been committed at the end of the completion-queue pass.
  gDeferredSlots: seq[cint]

  # accept scratch (must stay valid until the accept op completes)
  gAcceptSa:    Sockaddr_in
  gAcceptSaLen: SockLen

  # timeout scratch (must stay valid until the timeout op completes)
  gTs:          KernelTimespec

# ---------------------------------------------------------------------------
# user_data packing

proc makeUd(tag: uint64, slot: cint, gen: uint32): uint64 {.inline.} =
  tag or (uint64(uint32(slot)) shl 8) or (uint64(gen) shl 32)

proc udTag(ud: uint64): uint64 {.inline.} = ud and 0xFF'u64
proc udSlot(ud: uint64): cint {.inline.} = cint((ud shr 8) and 0xFFFFFF'u64)
proc udGen(ud: uint64): uint32 {.inline.} = uint32(ud shr 32)

# ---------------------------------------------------------------------------
# HTTP/1.1 parsing and serialisation live in the shared `http` module (imported
# above); this transport only supplies its inbound body cap (MaxInBuf) to
# parseHead.

proc processRequests(c: var Conn): bool =
  ## Parse and answer complete pipelined requests, OUTPUT-bounded by MaxOutBuf
  ## (mirrors server.nim). Returns false when the framing is malformed and the
  ## connection must close. Stopping on the output cap leaves bytes in flight,
  ## so the send-drain path re-invokes this and finishes the burst.
  # Advance with a local offset and compact once at the end; front-deleting per
  # request was O(n^2) on a large pipelined buffer (see server.nim).
  var pos = 0
  while true:
    if c.outbuf.len - c.outOff >= MaxOutBuf:
      break
    let hend = c.inbuf.find("\r\n\r\n", pos)
    if hend < 0:
      if c.inbuf.len - pos > MaxHeader:
        return false
      break
    if hend - pos > MaxHeader:
      return false

    let head = parseHead(c.inbuf, pos, hend, MaxInBuf)
    if not head.ok:
      return false
    if head.bad:
      c.outbuf.add buildResponse(400, """{"error":"bad request"}""", false)
      c.closing = true
      return false
    if head.contentLen > MaxInBuf:
      return false

    let totalLen = (hend - pos) + 4 + head.contentLen
    if c.inbuf.len - pos < totalLen:
      break                  # body not fully arrived
    var rawBody = ""
    if head.contentLen > 0:
      let b = hend + 4
      rawBody = c.inbuf[b ..< b + head.contentLen]
    pos += totalLen

    var resp: ApiResponse
    try:
      resp = handleRequest(gDb, gSecret, head.httpMethod, head.path, head.auth, rawBody)
    except Exception as ex:
      # Exception also covers Defect: a request must never abort the process.
      stderr.writeLine("request handler error: " & ex.msg)
      resp = ApiResponse(status: 500, body: """{"error":"internal server error"}""")

    c.keepAlive = head.keepAlive
    let respStart = c.outbuf.len
    c.outbuf.add buildResponse(resp.status, resp.body, head.keepAlive)
    # Record responses produced while a group-commit batch is open: their SEND
    # must be withheld until that batch commits and becomes 500 if it fails.
    if gDb != nil and gDb.batchOpen():
      if c.batchCount == 0:
        c.batchFrom = respStart
      inc c.batchCount
    if not head.keepAlive:
      c.closing = true
      break

  if pos > 0:
    c.inbuf.delete(0 ..< pos)
  true

# ---------------------------------------------------------------------------
# fixed-file table bookkeeping

proc updateSlotRaw(slot: cint, fd: cint): int =
  ## IORING_REGISTER_FILES_UPDATE: install `fd` (or -1 to clear) at `slot`.
  ## Passing -1 drops the table's reference, which closes the socket once any
  ## in-flight op holding its own reference completes.
  var up: array[4, uint64]
  up[0] = uint64(uint32(slot))          # offset(u32) + resv(u32)
  var tmp = fd
  up[1] = uint64(cast[uint](addr tmp))  # pointer to the fd array
  let ret = c_syscall(SYS_io_uring_register, clong(gRing.fd),
                      clong(IORING_REGISTER_FILES_UPDATE),
                      clong(cast[uint](addr up[0])), clong(1))
  if ret < 0:
    result = -cint(osLastError().int)
  else:
    result = int(ret)

proc allocSlot(): cint =
  if gFreeTop <= 0:
    return -1
  dec gFreeTop
  gFreeSlots[gFreeTop]

proc releaseSlot(slot: cint) =
  ## Recv storage is pooled (not per-connection), so returning a slot is just
  ## bookkeeping: no buffer to free, only flags and the free-list push.
  ## In direct-accept mode the *kernel* owns slot allocation (FILES_UPDATE(-1) in
  ## closeConn already frees the slot), so we must NOT also push it onto the
  ## userspace free list - doing so would double-count and could overflow it.
  conns[int(slot)].wantFree = false
  conns[int(slot)].recvStarved = false
  if gAcceptDirect:
    return
  gFreeSlots[gFreeTop] = slot
  inc gFreeTop

proc rebuildFreeList() =
  ## Rebuild the userspace free-slot list from the live connection table. Used
  ## when a running ring downgrades from direct accept to the register-per-accept
  ## path: the list was never consumed in direct mode, so it is stale and must
  ## exclude every slot the kernel handed out directly.
  gFreeTop = 0
  for i in 1 ..< FixedSlots:
    if not conns[i].active:
      gFreeSlots[gFreeTop] = cint(i)
      inc gFreeTop

proc maybeFreeSlot(slot: cint) =
  if slot < 0:
    return
  if not conns[int(slot)].active and conns[int(slot)].wantFree and
     conns[int(slot)].pending == 0:
    releaseSlot(slot)

# ---------------------------------------------------------------------------
# submission helpers

proc mustSqe(): ptr Sqe =
  ## Reserve an SQE, flushing (and, if needed, waiting for a completion) when
  ## the submission queue is momentarily full.
  result = gRing.getSqe()
  if result.isNil:
    discard gRing.submit()
    result = gRing.getSqe()
  if result.isNil:
    discard gRing.submitAndWait(1)
    result = gRing.getSqe()

# ---------------------------------------------------------------------------
# provided-buffer pool
#
# Buffer `bid` always lives at `gBufBase[bid * BufSize]`; the id is the index,
# so a returned buffer is simply re-published with that same id and no
# allocation bookkeeping is needed at all on the hot path.

proc poolPush(bid: int) =
  ## Hand buffer `bid` back to the kernel. With a registered buf ring this is
  ## pure userspace bookkeeping (write the descriptor, publish `tail`); the
  ## fallback submits one IORING_OP_PROVIDE_BUFFERS per buffer.
  if gUseBufRing:
    let idx = int(gBufTail) and gBufMask
    let e = addr gBufRing[idx]
    e.`addr` = cast[uint64](addr gBufBase[bid * BufSize])
    e.len = uint32(BufSize)
    e.bid = uint16(bid)
    # Publish the producer tail only after the descriptor fields; the kernel
    # loads `tail` with acquire semantics, and on x86 stores are ordered.
    gBufTail = gBufTail + 1'u16
    gBufRing[0].resv = gBufTail
  else:
    let sqe = mustSqe()
    if sqe.isNil:
      return
    prepProvideBuffers(sqe, cast[uint64](addr gBufBase[bid * BufSize]),
                       uint32(BufSize), 1'u32, uint16(bid), BufGroup)
    sqe.user_data = TAG_PROVIDE
  inc gBufsReady

proc poolConsume() {.inline.} =
  dec gBufsReady
  inc gConsumedThisPass

proc armAccept() =
  ## Arm accept. With IORING_ACCEPT_MULTISHOT one SQE serves every pending
  ## connection until the kernel reports it can no longer continue
  ## (IORING_CQE_F_MORE clears); otherwise it is re-armed per completion.
  if gAcceptArmed:
    return
  let sqe = mustSqe()
  if sqe.isNil:
    return
  gAcceptSaLen = SockLen(sizeof(gAcceptSa))
  sqe.opcode = IORING_OP_ACCEPT
  sqe.flags = IOSQE_FIXED_FILE
  sqe.fd = ListenSlot
  sqe.`addr` = cast[uint64](addr gAcceptSa)
  sqe.off = cast[uint64](addr gAcceptSaLen)
  if gAcceptDirect and gAcceptMultishot:
    # Direct descriptor: the kernel installs the accepted socket into the first
    # free registered-file slot and returns that SLOT in the CQE's `res` (field
    # is the splice_fd_in/file_index union at Sqe offset 44). SOCK_CLOEXEC is
    # rejected (-EINVAL) here because a fixed descriptor has no process fd, so
    # only SOCK_NONBLOCK is requested; the non-direct path keeps both.
    sqe.rw_flags = uint32(SOCK_NONBLOCK)
    sqe.splice_fd_in = cast[int32](IORING_FILE_INDEX_ALLOC)
  else:
    sqe.rw_flags = uint32(SOCK_NONBLOCK or SOCK_CLOEXEC)
  if gAcceptMultishot:
    sqe.ioprio = uint16(IORING_ACCEPT_MULTISHOT)
  sqe.user_data = makeUd(TAG_ACCEPT, 0, 0)
  gAcceptArmed = true

proc armRecv(slot: cint) =
  ## Arm (or, in multishot mode, leave armed) a recv that selects its landing
  ## buffer from the shared provided-buffer pool. `IOSQE_BUFFER_SELECT` makes
  ## the kernel pick a buffer; the CQE then carries its id in the high 16 bits
  ## of cqe->flags.
  if slot < 0:
    return
  var c = addr conns[int(slot)]
  if not c.active or c.closing or c.recvArmed:
    return
  let sqe = mustSqe()
  if sqe.isNil:
    return
  sqe.opcode = IORING_OP_RECV
  sqe.flags = IOSQE_FIXED_FILE or IOSQE_BUFFER_SELECT
  sqe.fd = slot
  sqe.buf_index = BufGroup
  sqe.len = uint32(BufSize)
  if gRecvMultishot:
    sqe.ioprio = uint16(IORING_RECV_MULTISHOT)
  sqe.user_data = makeUd(TAG_RECV, slot, c.gen)
  c.recvArmed = true
  c.recvStarved = false
  inc c.pending

proc rearmStarved() =
  ## Re-arm every recv that ended with -ENOBUFS, now that buffers have returned.
  ## Only called when at least one buffer is available (gBufsReady > 0), so it
  ## cannot spin on a still-empty pool.
  if gRecvStarved.len == 0:
    return
  let starved = gRecvStarved
  gRecvStarved.setLen(0)
  for slot in starved:
    if slot < 0 or int(slot) >= conns.len:
      continue
    var c = addr conns[int(slot)]
    if c.active and not c.closing and not c.recvArmed and not c.readPaused:
      armRecv(slot)
    else:
      c.recvStarved = false

proc armSend(slot: cint) =
  if slot < 0:
    return
  var c = addr conns[int(slot)]
  if not c.active or c.sendArmed:
    return
  if c.outOff >= c.outbuf.len:
    return
  let sqe = mustSqe()
  if sqe.isNil:
    return
  sqe.opcode = IORING_OP_SEND
  sqe.flags = IOSQE_FIXED_FILE
  sqe.fd = slot
  sqe.`addr` = cast[uint64](addr c.outbuf[c.outOff])
  sqe.len = uint32(c.outbuf.len - c.outOff)
  sqe.rw_flags = uint32(MSG_NOSIGNAL)
  sqe.user_data = makeUd(TAG_SEND, slot, c.gen)
  c.sendArmed = true
  inc c.pending

proc armSendOrDefer(slot: cint) =
  ## Arm a SEND for freshly produced output, unless that output was produced
  ## while a group-commit batch is open: then the response must wait for COMMIT,
  ## so remember the slot and let `flushDeferredSlots` arm the send afterwards.
  if slot < 0:
    return
  var c = addr conns[int(slot)]
  if not c.active or c.sendArmed:
    return
  if c.outOff >= c.outbuf.len:
    return
  if gDb != nil and gDb.batchOpen() and c.batchCount > 0:
    if not c.deferred:
      c.deferred = true
      gDeferredSlots.add slot
    return
  armSend(slot)

proc cancelOp(slot: cint, tag: uint64, gen: uint32) =
  ## Cancel an in-flight op by its user_data so the socket's last reference can
  ## be dropped and the fixed slot reused.
  let sqe = mustSqe()
  if sqe.isNil:
    return
  sqe.opcode = IORING_OP_ASYNC_CANCEL
  sqe.`addr` = makeUd(tag, slot, gen)
  sqe.rw_flags = IORING_ASYNC_CANCEL_USERDATA
  sqe.user_data = TAG_CANCEL

proc disarmRecv(slot: cint) =
  ## Stop a still-armed (multishot) recv so it cannot keep delivering data after
  ## the connection paused reading or decided to close. The op stays counted in
  ## `pending` until its -ECANCELED completion arrives.
  if slot < 0 or int(slot) >= conns.len:
    return
  var c = addr conns[int(slot)]
  if c.recvArmed:
    c.recvArmed = false
    cancelOp(slot, TAG_RECV, c.gen)

proc armTimeout() =
  if gTimeoutArmed:
    return
  let sqe = mustSqe()
  if sqe.isNil:
    return
  gTs.tv_sec = int64(TickSeconds)
  gTs.tv_nsec = 0
  sqe.opcode = IORING_OP_TIMEOUT
  sqe.fd = -1'i32
  sqe.`addr` = cast[uint64](addr gTs)
  sqe.len = 1'u32
  sqe.off = 0
  sqe.user_data = TAG_TIMEOUT
  gTimeoutArmed = true

# ---------------------------------------------------------------------------
# connection lifecycle

proc closeConn(slot: cint) =
  if slot < 0 or int(slot) >= conns.len:
    return
  var c = addr conns[int(slot)]
  if not c.active:
    return
  c.active = false
  c.closing = true
  if c.recvArmed:
    cancelOp(slot, TAG_RECV, c.gen)
  if c.sendArmed:
    cancelOp(slot, TAG_SEND, c.gen)
  # Drop the table's reference; the socket closes once in-flight ops finish.
  discard updateSlotRaw(slot, -1'i32)
  # Release (not just truncate) so a pooled slot keeps no large capacity.
  c.inbuf = ""
  c.outbuf = ""
  c.outOff = 0
  c.batchFrom = 0
  c.batchCount = 0
  c.deferred = false
  c.wantFree = true
  dec openCount
  maybeFreeSlot(slot)

proc finalizeConn(slot: cint) =
  if slot < 0:
    return
  var c = addr conns[int(slot)]
  if not c.active:
    return
  if c.closing and not c.sendArmed and c.outOff >= c.outbuf.len:
    closeConn(slot)

# ---------------------------------------------------------------------------
# completion handlers

proc initAcceptedSlot(slot: cint) =
  ## Bring a freshly acquired fixed-file slot into service.  Recv storage is
  ## pooled, so there is nothing per-connection to allocate here.
  var c = addr conns[int(slot)]
  c.active = true
  c.slot = slot
  inc gGen
  c.gen = gGen
  c.inbuf.setLen(0)
  c.outbuf.setLen(0)
  c.outOff = 0
  c.keepAlive = true
  c.closing = false
  c.readPaused = false
  c.recvArmed = false
  c.sendArmed = false
  c.wantFree = false
  c.pending = 0
  c.recvStarved = false
  c.deferred = false
  c.batchFrom = 0
  c.batchCount = 0
  c.lastActive = gNow
  inc openCount

proc onAccept(res: cint, more: bool) =
  ## `more` mirrors IORING_CQE_F_MORE: while set, a multishot accept is still
  ## armed and must NOT be re-submitted.  On success `res` is either an OS fd
  ## (register-per-accept path) or a fixed-file SLOT index (direct path).
  if not more:
    gAcceptArmed = false
  if res >= 0:
    var slot: cint
    if gAcceptDirect:
      # Direct accept: the kernel already installed the socket into a free
      # fixed-file slot, and `res` IS that slot. No allocSlot(), no
      # FILES_UPDATE, no per-accept fd to close.
      slot = cint(res)
      if slot <= 0 or int(slot) >= conns.len or conns[int(slot)].active:
        stderr.writeLine("io_uring: direct accept returned unusable slot " & $slot)
        if slot > 0 and int(slot) < conns.len:
          discard updateSlotRaw(slot, -1'i32)   # drop the kernel-installed file
        if not more:
          armAccept()
        return
      gAcceptDirectProven = true
    else:
      slot = allocSlot()
      if slot < 0:
        discard posix.close(res)
        gAcceptPaused = true          # retry on the next tick
        return
      if updateSlotRaw(slot, res) < 0:
        stderr.writeLine("io_uring: register update failed; dropping accept")
        discard posix.close(res)
        gFreeSlots[gFreeTop] = slot; inc gFreeTop
        if not more:
          armAccept()
        return
      # Registered: the fixed table now holds its own reference, so release the
      # process fd immediately (this is the RLIMIT_NOFILE bypass).
      discard posix.close(res)

    gAcceptMsProven = true
    initAcceptedSlot(slot)
    armRecv(slot)
    if not more:
      armAccept()
  else:
    if res == -EAGAIN or res == -EINTR:
      armAccept()
    elif res == -EINVAL or res == -EOPNOTSUPP:
      if gAcceptDirect and gAcceptMultishot and not gAcceptDirectProven:
        # Kernel refuses IORING_FILE_INDEX_ALLOC direct accept: keep multishot
        # but go back to registering each accepted fd into a free slot.
        gAcceptDirect = false
        rebuildFreeList()
        stderr.writeLine("io_uring: direct accept (FILE_INDEX_ALLOC) unsupported (" &
                         $res & "); falling back to register-per-accept")
        armAccept()
      elif gAcceptMultishot and not gAcceptMsProven:
        # Kernel refuses IORING_ACCEPT_MULTISHOT: degrade to a re-armed one-shot
        # accept. This is the only way to detect it (accept flags are not
        # covered by IORING_REGISTER_PROBE).
        gAcceptMultishot = false
        gAcceptDirect = false
        stderr.writeLine("io_uring: multishot accept unsupported (" & $res &
                         "); falling back to re-armed accept")
        armAccept()
      else:
        gAcceptPaused = true
    elif res == -ECANCELED:
      discard
    else:
      # EMFILE/ENFILE/ECONNABORTED/... : back off; the tick revives the accept.
      gAcceptPaused = true

proc onRecv(slot: cint, gen: uint32, res: cint, cflags: uint32) =
  ## One recv completion. In multishot mode the same SQE yields many of these
  ## (each with IORING_CQE_F_MORE); only the terminal one clears `pending`. In
  ## both modes the data lives in a provided buffer whose id is in the high 16
  ## bits of `cflags`, and which is returned to the pool before we leave.
  if slot < 0 or int(slot) >= conns.len:
    return
  let hasBuf = (cflags and IORING_CQE_F_BUFFER) != 0'u32
  let more   = (cflags and IORING_CQE_F_MORE) != 0'u32
  var bid = -1
  if hasBuf:
    bid = int((cflags shr uint32(IORING_CQE_BUFFER_SHIFT)) and 0xFFFF'u32)
    poolConsume()

  if conns[int(slot)].gen != gen:
    # Stale completion for a reused slot: only the buffer needs returning.
    if bid >= 0: poolPush(bid)
    return

  var c = addr conns[int(slot)]
  if not more:
    # Terminal completion: this recv op is no longer armed.
    c.recvArmed = false
    dec c.pending

  if not c.active:
    if bid >= 0: poolPush(bid)
    maybeFreeSlot(slot)
    return
  c.lastActive = gNow

  if res > 0:
    if c.closing:
      if bid >= 0: poolPush(bid)      # discard data for a closing conn
      return
    let n = int(res)
    let old = c.inbuf.len
    c.inbuf.setLen(old + n)
    if bid >= 0:
      copyMem(addr c.inbuf[old], addr gBufBase[bid * BufSize], n)
      poolPush(bid)
    c.recvStarved = false
    # Stop reading (do not close) once the buffer passes the request cap so a
    # pipelined burst drains; only a lone oversized request/header is rejected.
    if c.inbuf.len > MaxRequest:
      if findHeaderEnd(c.inbuf) >= 0:
        c.readPaused = true
      else:
        c.closing = true
    if not c.closing and not c.sendArmed:
      if not processRequests(c[]):
        c.closing = true
      if c.inbuf.len <= MaxRequest:
        c.readPaused = false
      # Defer the SEND when this pass opened a write batch (see armSendOrDefer).
      armSendOrDefer(slot)
    if not c.closing and not c.readPaused:
      armRecv(slot)          # no-op while a multishot op is still armed
    elif c.recvArmed:
      disarmRecv(slot)
    finalizeConn(slot)
    return

  # Non-data completion.
  if bid >= 0: poolPush(bid)
  if res == 0:
    # Peer closed its write side: flush what we have, then close.
    c.closing = true
    if not c.sendArmed:
      armSend(slot)
    finalizeConn(slot)
    return
  # res < 0
  if res == -ENOBUFS:
    # The pool is momentarily empty. If a buffer is already available, re-arm
    # now; otherwise remember the slot and re-arm once buffers return (end of
    # pass / tick), which prevents a submit-and-immediately-ENOBUFS spin.
    if gBufsReady > 0:
      armRecv(slot)
    else:
      c.recvStarved = true
      gRecvStarved.add slot
    return
  if res == -EAGAIN or res == -EINTR:
    armRecv(slot)
    return
  if res == -ECANCELED or res == -ENOENT:
    return
  c.closing = true
  if not c.sendArmed:
    armSend(slot)
  finalizeConn(slot)

proc onSend(slot: cint, gen: uint32, res: cint) =
  if slot < 0 or int(slot) >= conns.len:
    return
  if conns[int(slot)].gen != gen:
    return
  dec conns[int(slot)].pending
  if not conns[int(slot)].active:
    maybeFreeSlot(slot)
    return
  var c = addr conns[int(slot)]
  c.sendArmed = false
  c.lastActive = gNow

  if res > 0:
    c.outOff += int(res)
  elif res == 0:
    discard
  else:
    if res == -EAGAIN:
      armSend(slot)
      return
    if res == -ENOBUFS:
      # Transient kernel allocation failure: retry on the next tick rather than
      # spin, so the response is recovered instead of the connection dropped.
      c.sendArmed = false
      gSendRetry.add slot
      return
    if res == -ECANCELED or res == -ENOENT:
      return
    closeConn(slot)
    return

  if c.outOff >= c.outbuf.len:
    c.outbuf.setLen(0)
    c.outOff = 0
    # Answer any request that arrived while the previous response was in flight.
    if not c.closing and c.inbuf.len > 0:
      if not processRequests(c[]):
        c.closing = true
      if c.inbuf.len <= MaxRequest:
        c.readPaused = false
      # Defer the SEND when this pass opened a write batch (see armSendOrDefer).
      armSendOrDefer(slot)
    if c.closing and not c.sendArmed and c.outOff >= c.outbuf.len:
      closeConn(slot)
      return
    if not c.closing and not c.readPaused:
      armRecv(slot)
  else:
    armSend(slot)   # partial send: push the remainder

proc sweep()   # forward declaration (defined below)

proc onTimeout() =
  gTimeoutArmed = false
  # Idle/stall sweep moved here from the per-pass hot path: walking all
  # FixedSlots on every completion batch was pure overhead; the 1 s timer
  # already matches the epoll server's sweep cadence.
  sweep()
  # Retry sends that hit a transient -ENOBUFS (never dropped, never spun on).
  if gSendRetry.len > 0:
    let pend = gSendRetry
    gSendRetry.setLen(0)
    for slot in pend:
      if slot >= 0 and int(slot) < conns.len:
        var c = addr conns[int(slot)]
        if c.active and not c.sendArmed and c.outOff < c.outbuf.len:
          armSend(slot)
  # Re-arm recvs starved of provided buffers.
  if gRecvStarved.len > 0 and gBufsReady > 0:
    rearmStarved()
  if gAcceptPaused:
    gAcceptPaused = false
    armAccept()
  elif not gAcceptArmed:
    armAccept()
  armTimeout()

# ---------------------------------------------------------------------------
# idle / stall sweep (mirrors the epoll server's policy)

proc sweep() =
  for slot in 0 ..< conns.len:
    if conns[slot].active:
      let pendingOut = conns[slot].sendArmed or
                       conns[slot].outOff < conns[slot].outbuf.len
      let midReq = conns[slot].inbuf.len > 0
      let limit = if pendingOut or midReq: StallTimeout else: IdleKeepAlive
      if gNow - conns[slot].lastActive > limit:
        closeConn(cint(slot))

# ---------------------------------------------------------------------------
# reaping

proc dispatch(ud: uint64, res: cint, cflags: uint32) =
  case udTag(ud)
  of TAG_ACCEPT:  onAccept(res, (cflags and IORING_CQE_F_MORE) != 0'u32)
  of TAG_RECV:    onRecv(udSlot(ud), udGen(ud), res, cflags)
  of TAG_SEND:    onSend(udSlot(ud), udGen(ud), res)
  of TAG_TIMEOUT: onTimeout()
  of TAG_CANCEL:  discard
  of TAG_PROVIDE: discard
  else:           discard

proc flushDeferredSlots(commitOk: bool) =
  ## Release the SENDs withheld during this pass, once the pass's batch has been
  ## committed (or has failed). On a failed COMMIT every response produced against
  ## the rolled-back batch is replaced by a 500, so no success byte for an
  ## uncommitted write can reach the socket.
  for slot in gDeferredSlots:
    if slot < 0 or int(slot) >= conns.len:
      continue
    var c = addr conns[int(slot)]
    c.deferred = false
    if not c.active:
      continue
    if not commitOk and c.batchCount > 0:
      c.outbuf.setLen(c.batchFrom)
      for _ in 0 ..< c.batchCount:
        c.outbuf.add buildResponse(500, """{"error":"internal server error"}""",
                                   c.keepAlive)
      c.closing = true
    c.batchFrom = 0
    c.batchCount = 0
    if c.outOff < c.outbuf.len:
      armSend(slot)
    if c.closing and not c.sendArmed and c.outOff >= c.outbuf.len:
      closeConn(slot)
  gDeferredSlots.setLen(0)

proc runLoop() =
  ## One thread, one ring. Per pass we drain the *entire* completion queue in a
  ## single loop, do the send-side work, then loop. The end-of-pass submit is
  ## deliberately omitted: `submitAndWait(1)` at the top of the next iteration
  ## flushes every SQE reserved during the drain, so the steady-state hot loop
  ## costs exactly ONE io_uring_enter per pass (plus whatever `mustSqe()` is
  ## forced to flush if the SQ genuinely fills mid-drain).
  armAccept()
  armTimeout()

  while true:
    try:
      discard gRing.submitAndWait(1)   # flush reserved SQEs + block for >=1 CQE
    except IoRingError as ex:
      if ex.errno == EINTR:
        continue
      stderr.writeLine("io_uring_enter failed: " & ex.msg)
      quit(1)

    gNow = epochTime()
    gConsumedThisPass = 0
    while true:
      let cqe = gRing.peekCqe()
      if cqe.isNil:
        break
      let ud = cqe.user_data
      let res = cqe.res
      let fl = cqe.flags
      gRing.cqeSeen()
      dispatch(ud, res, fl)

    # End of one completion-queue pass: commit the pass's write batch (if any)
    # and only then arm the SENDs withheld for it (rule 6: durable before send).
    if gDb != nil and gDb.batchOpen():
      flushDeferredSlots(gDb.commitBatch())
    elif gDeferredSlots.len > 0:
      flushDeferredSlots(true)

    # Buffers returned during this pass may have un-starved recvs that hit
    # -ENOBUFS earlier in the same pass.
    if gRecvStarved.len > 0 and gConsumedThisPass > 0 and gBufsReady > 0:
      rearmStarved()

# ---------------------------------------------------------------------------
# setup

proc raiseNofile() =
  ## Raise this process's soft RLIMIT_NOFILE to its hard limit before registering
  ## the fixed-file table. `IORING_REGISTER_FILES` needs one fd slot per entry, so
  ## a 65535 soft limit cannot accommodate a 65536-entry table; raising the soft
  ## limit (per-process, no sysctl) removes that mismatch, and FixedSlots is also
  ## kept below the common 65535 ceiling. No-op when soft == hard.
  var rl: RLimit
  if getrlimit(RLIMIT_NOFILE, rl) != 0:
    stderr.writeLine("getrlimit(RLIMIT_NOFILE) failed: errno=" & $(lastErrno()))
    return
  let soft0 = rl.rlim_cur
  let hard = rl.rlim_max
  if soft0 < hard:
    var want: RLimit
    want.rlim_cur = hard
    want.rlim_max = hard
    if setrlimit(RLIMIT_NOFILE, want) == 0:
      var after: RLimit
      discard getrlimit(RLIMIT_NOFILE, after)
      stderr.writeLine("RLIMIT_NOFILE raised: soft " & $soft0 & " -> " &
                       $after.rlim_cur & " (hard " & $after.rlim_max & ")")
    else:
      stderr.writeLine("setrlimit(RLIMIT_NOFILE) failed: errno=" & $(lastErrno()))
  else:
    stderr.writeLine("RLIMIT_NOFILE soft==hard==" & $soft0 & " (nothing to raise)")

proc setupListener(cfg: Config): cint =
  let fd = cint(posix.socket(posix.AF_INET,
                             posix.SOCK_STREAM or SOCK_NONBLOCK, 0))
  if fd < 0:
    stderr.writeLine("socket() failed: " & $(lastErrno()))
    return -1
  var one: cint = 1
  discard posix.setsockopt(SocketHandle(fd), posix.SOL_SOCKET,
                           posix.SO_REUSEADDR, addr one, SockLen(sizeof(one)))
  var sa: Sockaddr_in
  sa.sin_family = TSa_Family(posix.AF_INET)
  sa.sin_port = nativesockets.htons(uint16(cfg.port))
  if posix.inet_pton(posix.AF_INET, cfg.host.cstring, addr sa.sin_addr) != 1:
    stderr.writeLine("invalid HOST: " & cfg.host)
    discard posix.close(fd)
    return -1
  if posix.bindSocket(SocketHandle(fd), cast[ptr SockAddr](addr sa),
                      SockLen(sizeof(sa))) != 0:
    stderr.writeLine("bind() failed on " & cfg.host & ":" & $cfg.port &
                     ": " & $(lastErrno()))
    discard posix.close(fd)
    return -1
  if posix.listen(SocketHandle(fd), cint(ListenBacklog)) != 0:
    stderr.writeLine("listen() failed: " & $(lastErrno()))
    discard posix.close(fd)
    return -1
  fd

proc groupCommitEnabled(): bool =
  ## GROUP_COMMIT defaults ON; only "0" (or unparseable -> default) is treated
  ## as a disable request, matching the epoll server's envInt semantics.
  try:
    result = parseInt(getEnv("GROUP_COMMIT", "1")) != 0
  except ValueError:
    result = true

proc envIsOneshot(name: string): bool =
  ## IOURING_RECV / IOURING_ACCEPT: default "multishot"; anything else (notably
  ## "oneshot") selects the one-shot path, which is the p99 fallback.
  getEnv(name, "multishot").toLowerAscii == "oneshot"

proc initPool() =
  ## Allocate the shared provided-buffer pool and publish it to the kernel.
  ## The data region is one lazily-faulted `alloc`: untouched pages stay out of
  ## RSS, and because buffers are only taken while bytes are in flight, the
  ## resident cost tracks peak concurrent receives rather than connection count.
  # Live pool size (IOURING_BUFS, default BufRingDefault): rounded down to a power
  # of two and clamped, so the buf-ring requirement always holds. Exposed so the
  # -ENOBUFS path can be exercised (a tiny pool) without a rebuild.
  var want = BufRingDefault
  try:
    want = parseInt(getEnv("IOURING_BUFS", $BufRingDefault))
  except ValueError:
    want = BufRingDefault
  if want < 64: want = 64
  if want > BufRingMax: want = BufRingMax
  var p2 = 1
  while p2 * 2 <= want: p2 *= 2
  gBufSlots = p2
  gBufMask = p2 - 1

  let bytes = gBufSlots * BufSize
  gBufBase = cast[ptr UncheckedArray[char]](alloc(bytes))
  if gBufBase.isNil:
    stderr.writeLine("io_uring: buffer pool alloc failed (" & $bytes & " bytes)")
    quit(1)

  # The descriptor ring must be page aligned; mmap gives us that for free.
  let ringBytes = gBufSlots * sizeof(BufRingEntry)
  let p = posix.mmap(nil, ringBytes, PROT_READ or PROT_WRITE,
                     MAP_PRIVATE or MAP_ANONYMOUS, -1, Off(0))
  if p.isNil or p == MAP_FAILED:
    stderr.writeLine("io_uring: buf-ring mmap failed (" & $ringBytes & " bytes)")
    quit(1)
  gBufRing = cast[ptr UncheckedArray[BufRingEntry]](p)

  # IOURING_BUFRING=0 forces the IORING_OP_PROVIDE_BUFFERS path (for testing the
  # degradation); otherwise we try the registered ring and fall back on failure.
  if getEnv("IOURING_BUFRING", "1") == "0":
    gUseBufRing = false
    stderr.writeLine("io_uring: IOURING_BUFRING=0; using IORING_OP_PROVIDE_BUFFERS")
  else:
    try:
      discard gRing.registerBufRing(p, uint32(gBufSlots), BufGroup)
      gUseBufRing = true
    except IoRingError as ex:
      gUseBufRing = false
      stderr.writeLine("io_uring: PBUF_RING unavailable (" & ex.msg &
                       "); falling back to IORING_OP_PROVIDE_BUFFERS")

  gBufTail = 0
  gBufsReady = 0
  if gUseBufRing:
    for bid in 0 ..< gBufSlots:
      poolPush(bid)
  else:
    # One SQE hands the whole list to buffer group BufGroup, ids 0..N-1.
    let sqe = mustSqe()
    if sqe.isNil:
      stderr.writeLine("io_uring: could not submit PROVIDE_BUFFERS")
      quit(1)
    prepProvideBuffers(sqe, cast[uint64](addr gBufBase[0]), uint32(BufSize),
                       uint32(gBufSlots), 0'u16, BufGroup)
    sqe.user_data = TAG_PROVIDE
    gBufsReady = gBufSlots
    discard gRing.submit()

  stderr.writeLine("io_uring pool: " & $gBufSlots & " x " & $BufSize &
                   " B (" & $(bytes div (1024 * 1024)) & " MiB, " &
                   (if gUseBufRing: "registered buf_ring"
                    else: "provide_buffers list") & ")")

# ---------------------------------------------------------------------------
# Last-resort fallback for the one failure the in-ring degradation cannot cover.
#
# Every per-feature failure (flagged setup, multishot recv, multishot/direct
# accept, buf_ring) degrades in place with the server staying up. The only
# unrecoverable case is `io_uring_setup` itself failing outright - ENOSYS on a
# kernel/VM without io_uring, EPERM under seccomp, ENOMEM for the ring. There
# the transport simply does not exist. Rather than leave the service down, execv
# the sibling epoll server that build.sh always produces next to this binary
# (same directory), replacing the image in place so the process/PID, environment
# and inherited descriptors are preserved. If the sibling is missing we exit
# non-zero with a clear message. `getAppFilename` resolves /proc/self/exe, so the
# path is correct however the binary was invoked.
proc fallbackToEpoll(reason: string) =
  let epoll = parentDir(getAppFilename()) / "server"
  stderr.writeLine("io_uring_setup failed (" & reason &
                   "); falling back to sibling epoll server: " & epoll)
  if fileExists(epoll) and not dirExists(epoll):
    var argv: array[2, cstring]
    argv[0] = epoll.cstring
    argv[1] = nil
    # execv replaces the process image and returns only on failure.
    discard execv(epoll.cstring, cast[cstringArray](addr argv[0]))
    stderr.writeLine("execv(" & epoll & ") failed: errno=" & $(lastErrno()))
  else:
    stderr.writeLine("no sibling epoll server at " & epoll & "; exiting")
  quit(1)

proc main() =
  let cfg = loadConfig()
  gSecret = cfg.jwtSecret

  discard posix.signal(SIGPIPE, SIG_IGN)

  # Turbo toggles: multishot by default, one env var away from the safe path.
  gRecvMultishot = not envIsOneshot("IOURING_RECV")
  gAcceptMultishot = not envIsOneshot("IOURING_ACCEPT")
  # Direct accept (kernel-installed fixed slots) defaults ON;
  # IOURING_ACCEPT_DIRECT=0 forces the register-per-accept path so the fallback
  # can be exercised.
  gAcceptDirect = getEnv("IOURING_ACCEPT_DIRECT", "1") != "0"
  if not gAcceptMultishot:
    gAcceptDirect = false

  # Raise the fd ceiling BEFORE registering the fixed-file table (see raiseNofile).
  raiseNofile()

  # ---- the ring -----------------------------------------------------------
  # SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN: the recommended setup for a
  # single-threaded, per-thread ring. Completion (task) work is deferred to
  # io_uring_enter, which runLoop calls every pass. A kernel that rejects the
  # flags is retried bare inside setupQueue, so startup never fails on a hint.
  const RingSetupFlags = IORING_SETUP_SINGLE_ISSUER or
                         IORING_SETUP_DEFER_TASKRUN or
                         IORING_SETUP_COOP_TASKRUN
  try:
    gRing = setupQueue(RingEntries, RingSetupFlags)
  except IoRingError as ex:
    # Unrecoverable: the ring itself is unavailable. Hand off to the epoll server.
    fallbackToEpoll(ex.msg)
  if gRing.flagsDropped:
    stderr.writeLine("io_uring setup: flags 0x" & toHex(gRing.requestedFlags) &
                     " rejected; running WITHOUT SINGLE_ISSUER|DEFER_TASKRUN")
  else:
    stderr.writeLine("io_uring setup: flags 0x" & toHex(gRing.usedFlags) &
                     " in effect (SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN)")

  # ---- the provided-buffer pool -------------------------------------------
  initPool()

  # ---- listener -----------------------------------------------------------
  let lfd = setupListener(cfg)
  if lfd < 0:
    quit(1)

  # ---- register the fixed-file table: slot 0 = listener, rest empty --------
  var fds = newSeq[cint](FixedSlots)
  for i in 0 ..< FixedSlots:
    fds[i] = -1'i32
  fds[0] = lfd
  try:
    discard gRing.registerFiles(fds)
  except IoRingError as ex:
    stderr.writeLine("IORING_REGISTER_FILES failed: " & ex.msg)
    quit(1)
  gListenRawFd = lfd
  # The registered reference keeps the listening socket alive, so the process
  # fd can be released: accepts then address it as fixed slot 0.
  discard posix.close(lfd)

  # ---- database (same call pattern as the epoll server) -------------------
  gDb = openDb(cfg.sqlitePath)
  # Write batching across one completion-queue pass; GROUP_COMMIT=0 restores
  # autocommit. Default ON.
  gDb.setGroupCommit(groupCommitEnabled())
  markStart()

  # ---- slot pools ---------------------------------------------------------
  conns = newSeq[Conn](FixedSlots)
  gFreeSlots = newSeq[cint](FixedSlots - 1)
  for i in 1 ..< FixedSlots:
    gFreeSlots[i - 1] = cint(i)
  gFreeTop = FixedSlots - 1

  stderr.writeLine("io_uring transport: entries=" & $RingEntries &
                   " fixed_file_slots=" & $FixedSlots &
                   " recv=" & (if gRecvMultishot: "multishot" else: "oneshot") &
                   " accept=" & (if not gAcceptMultishot: "oneshot"
                                elif gAcceptDirect: "multishot+direct"
                                else: "multishot"))

  runLoop()

main()
