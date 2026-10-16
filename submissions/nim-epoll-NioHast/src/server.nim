## Raw, single-threaded, edge-triggered epoll HTTP/1.1 server.
##
## Event-loop design
## -----------------
## There is one thread, one epoll instance (EPOLL_CLOEXEC) and one non-blocking
## listening socket. The listener is registered level-triggered (EPOLLIN) so we
## always notice a pending accept; every accepted connection is registered
## edge-triggered (EPOLLIN|EPOLLET) and owns a userspace input/output buffer.
##
## Invariants
## ----------
## * Drain-to-EAGAIN reads. Under edge-triggered epoll a single EPOLLIN edge
##   means "this fd is readable now"; if we did not consume every readable byte
##   the kernel would never re-arm the edge and the request would stall. So
##   `readConn` loops `recv` into a 64 KiB stack buffer until EAGAIN, appending
##   to the connection's input buffer. The accept path also immediately drains a
##   freshly accepted fd, closing the classic ET accept race (data that arrived
##   before the fd was added to the interest set).
## * Never block. recv/send/accept are all non-blocking; an EAGAIN is never
##   turned into a busy loop. send loops until EAGAIN and then yields.
## * Backpressure via EPOLLOUT. If a response cannot be flushed in full the
##   remaining bytes stay buffered, EPOLLOUT is added to the fd's interest set,
##   and the connection is revisited when the socket becomes writable. When the
##   output buffer drains, EPOLLOUT is removed again. Since we always attempt an
##   immediate flush before arming EPOLLOUT, a socket that is already writable
##   can never be missed by an ET edge.
## * Pipelining. After each read we parse *all* complete requests currently
##   buffered, append each response to the output buffer, then flush once.
## * Resource guards. A header block is at most 16 KiB; an input buffer that
##   grows past 1 MiB without yielding a complete request is malformed and the
##   connection is closed. Keep-alive sockets idle for 75 s (or stalled mid
##   request for 30 s) are swept every ~1 s and dropped, so idle fds never leak.
## * Sparse fd-indexed state. Per-connection state lives in a `seq[Conn]`
##   indexed by fd - small and pooled: a freed slot is reset and reused when the
##   kernel recycles the descriptor.
## * SIGPIPE is ignored process-wide and MSG_NOSIGNAL is passed on every send,
##   so a peer that vanishes mid-write cannot kill the loop.
##
## This process is capped by the kernel at RLIMIT_NOFILE descriptors (65535 on
## the challenge box), which is the real ceiling on concurrently open
## connections; the idle keep-alive eviction + RST path is what keeps the server
## serving well past that number.

import std/epoll
import std/posix except accept4
import std/nativesockets
import std/[os, strutils, times]

import config, db, api, http

# ---------------------------------------------------------------------------
# syscall surface that std/posix does not expose (or that we want verbatim)

var SOCK_NONBLOCK {.importc, header: "<sys/socket.h>".}: cint
var SOCK_CLOEXEC  {.importc, header: "<sys/socket.h>".}: cint
# TCP_DEFER_ACCEPT (value in seconds) is in <netinet/tcp.h>; a macro constant
# declared the same way as SOCK_NONBLOCK/SOCK_CLOEXEC above. Applied to the
# LISTEN socket.
var TCP_DEFER_ACCEPT {.importc, header: "<netinet/tcp.h>".}: cint
proc accept4(a1: SocketHandle, a2: ptr SockAddr, a3: ptr SockLen,
             flags: cint): SocketHandle {.
  importc, header: "<sys/socket.h>".}

var errno {.importc, header: "<errno.h>".}: cint

const
  # epoll_create1 flag; numerically identical to O_CLOEXEC on Linux.
  EPOLL_CLOEXEC = O_CLOEXEC

const
  ReadChunk     = 64 * 1024
  MaxInBuf      = 128 * 1024
  # Upper bound on buffered input for one connection. readConn drains the socket
  # (edge-triggered discipline) but closes once unparsed input exceeds this, which
  # also bounds the output a pipelined burst can produce. Every complete request in
  # the buffer is processed each event, so processing never depends on a new edge
  # and cannot stall.
  MaxRequest    = MaxInBuf + MaxHeader + 4
  MaxEvents     = 1024
  ListenBacklog = 4096
  IdleKeepAlive = 75.0
  StallTimeout  = 30.0
  SweepMs       = 1000
  AcceptPause   = 1.0

  # Idle-connection eviction thresholds (overridable via EVICT_HIGH/EVICT_LOW).
  # Crossing HIGH triggers an LRU eviction pass that sheds idle keep-alive
  # sockets until the count falls back to LOW (hysteresis: the gap keeps the
  # pass rare instead of thrashing around a single edge).
  EvictHighDefault = 63000
  EvictLowDefault  = 56000
  # On EMFILE we try to reclaim at least this many idle fds before backing off.
  EmfileBatch      = 64

# SO_LINGER payload: {l_onoff=1, l_linger=0} makes close() send RST instead of
# FIN. See closeConnRst.
type Linger = object
  l_onoff:  cint
  l_linger: cint

# ---------------------------------------------------------------------------
# connection state

type
  Conn = object
    fd:         cint
    active:     bool
    inbuf:      string
    outbuf:     string
    outOff:     int
    keepAlive:  bool
    lastActive: float
    wantWrite:  bool
    closing:    bool   ## close once the output buffer has drained
    readPaused: bool   ## stopped reading at the input cap with data still pending
    prev:       int    ## intrusive LRU links (index into `conns`, -1 = none)
    next:       int
    inLru:      bool   ## true iff this slot is currently on the idle-LRU list
    deferred:   bool   ## output withheld until the pass's group commit finishes
    batchFrom:  int    ## outbuf offset where this pass's batch output begins
    batchCount: int    ## responses appended while the pass's batch was open

  FlushResult = enum frDrained, frPending, frError
  ReadResult  = enum rdOk, rdClosed, rdTooBig

var
  conns:         seq[Conn]
  gEpfd:         cint = -1
  gListenFd:     cint = -1
  gListenPaused: bool = false
  gListenResume: float = 0.0
  gDb:           Db
  gSecret:       string

  # Open-socket accounting: `openCount` is maintained in O(1) and must always
  # equal the number of `active` slots in `conns`.
  openCount:   int = 0

  # Idle LRU: head is the least-recently-active idle keep-alive socket (the
  # next eviction victim), tail the most recently active. Only fully-drained
  # keep-alive sockets are ever on this list, so eviction never touches a
  # connection with buffered input, pending output, or in-flight work.
  lruHead: int = -1
  lruTail: int = -1

  # Connections whose responses were withheld because they were produced while
  # a group-commit batch was open. Flushed (or rewritten to 500) once the pass's
  # batch commits, at the end of the epoll_wait pass. Empty whenever no batch is
  # open, so a read-only pass is byte-for-byte and latency-for-latency unchanged.
  gDeferredFds: seq[cint]

  # Eviction policy + observability.
  gEvictHigh:     int = EvictHighDefault
  gEvictLow:      int = EvictLowDefault
  gEvictTotal:    int = 0
  gEvictPasses:   int = 0
  gLastEvictLog:  float = 0.0

# ---------------------------------------------------------------------------
# idle LRU (intrusive doubly-linked list over `conns` indices)

proc isIdle(c: Conn): bool {.inline.} =
  ## A socket is an eviction candidate only when it is a live keep-alive
  ## connection with nothing buffered and nothing in flight.
  c.active and c.keepAlive and not c.closing and
    c.inbuf.len == 0 and
    not c.wantWrite and c.outOff >= c.outbuf.len

proc lruUnlink(idx: int) =
  if not conns[idx].inLru:
    return
  let p = conns[idx].prev
  let n = conns[idx].next
  if p != -1: conns[p].next = n else: lruHead = n
  if n != -1: conns[n].prev = p else: lruTail = p
  conns[idx].prev = -1
  conns[idx].next = -1
  conns[idx].inLru = false

proc lruPushTail(idx: int) =
  if conns[idx].inLru:
    return
  conns[idx].prev = lruTail
  conns[idx].next = -1
  if lruTail != -1: conns[lruTail].next = idx else: lruHead = idx
  lruTail = idx
  conns[idx].inLru = true

proc lruTouch(idx: int) =
  ## Mark `idx` as most-recently-active.
  if conns[idx].inLru:
    lruUnlink(idx)
  lruPushTail(idx)

proc lruUpdate(idx: int) =
  ## Reconcile LRU membership with the connection's current state: idle sockets
  ## move to the tail, anything with work (or closing) leaves the list.
  if isIdle(conns[idx]):
    lruTouch(idx)
  else:
    lruUnlink(idx)

# ---------------------------------------------------------------------------
# HTTP/1.1 parsing and serialisation live in the shared `http` module (imported
# above) so the epoll and io_uring transports emit byte-identical responses.
# This transport only supplies its inbound body cap (MaxInBuf) to parseHead.

# ---------------------------------------------------------------------------
# epoll bookkeeping

proc setInterest(c: Conn) =
  var ev: EpollEvent
  ev.events = uint32(EPOLLIN) or uint32(EPOLLET)
  if c.wantWrite:
    ev.events = ev.events or uint32(EPOLLOUT)
  ev.data.fd = c.fd
  discard epoll_ctl(gEpfd, EPOLL_CTL_MOD, c.fd, addr ev)

proc closeConnMode(fd: cint, rst: bool) =
  let idx = int(fd)
  if idx < 0 or idx >= conns.len or not conns[idx].active:
    return
  if rst:
    # SO_LINGER{1,0}: the kernel discards the send queue and emits RST on
    # close(). We use this ONLY for evicted idle keep-alive sockets: it avoids
    # TIME_WAIT buildup and lets the peer's conntrack entry expire quickly,
    # which is what makes surviving a very high connection churn cheap.
    var li = Linger(l_onoff: 1, l_linger: 0)
    discard posix.setsockopt(SocketHandle(fd), posix.SOL_SOCKET,
                             posix.SO_LINGER, addr li, SockLen(sizeof(li)))
  lruUnlink(idx)
  discard epoll_ctl(gEpfd, EPOLL_CTL_DEL, fd, nil)
  discard posix.close(fd)
  conns[idx].active = false
  # Release (not just truncate) the buffers so a pooled slot cannot retain a
  # megabyte of capacity across connection churn.
  conns[idx].inbuf = ""
  conns[idx].outbuf = ""
  conns[idx].outOff = 0
  conns[idx].wantWrite = false
  conns[idx].closing = false
  conns[idx].readPaused = false
  conns[idx].keepAlive = false
  conns[idx].prev = -1
  conns[idx].next = -1
  conns[idx].inLru = false
  conns[idx].deferred = false
  conns[idx].batchFrom = 0
  conns[idx].batchCount = 0
  dec openCount

proc closeConn(fd: cint) =
  ## Normal FIN close: used for `Connection: close` and idle/stall timeouts.
  closeConnMode(fd, rst = false)

proc closeConnRst(fd: cint) =
  ## RST close: used for eviction only (see closeConnMode).
  closeConnMode(fd, rst = true)

proc verifyAccounting() =
  ## Cheap drift check run only on (rare) eviction passes: `openCount` must
  ## always equal the number of active slots, otherwise a leaked fd would grow
  ## the table without bound and eventually fail under a low RLIMIT_NOFILE.
  var active = 0
  for i in 0 ..< conns.len:
    if conns[i].active:
      inc active
  if active != openCount:
    stderr.writeLine("ACCOUNTING DRIFT: openCount=" & $openCount &
                     " activeSlots=" & $active)

proc evictIdleTo(target: int, now: float): int =
  ## Close least-recently-active fully-idle keep-alive sockets until `openCount`
  ## drops to `target` or no idle candidate remains. O(1) per victim via the
  ## intrusive LRU; `isIdle` is re-checked as a defensive guard so a slot that
  ## became busy can never be evicted (it is merely dropped from the list).
  ## Returns the number of connections closed.
  while openCount > target and lruHead != -1:
    let victim = lruHead
    if not isIdle(conns[victim]):
      lruUnlink(victim)          # stale entry: drop it, do not close
      continue
    closeConnRst(cint(victim))
    inc result
  if result > 0:
    gEvictTotal += result
    inc gEvictPasses
    # Throttle logging so a burst of EMFILE passes cannot flood (and block on)
    # stderr; the first pass always logs because gLastEvictLog starts at 0.
    if now - gLastEvictLog >= 0.5:
      gLastEvictLog = now
      stderr.writeLine("evict: freed=" & $result & " openCount=" & $openCount &
                       " evicted_total=" & $gEvictTotal &
                       " passes=" & $gEvictPasses)
      verifyAccounting()

proc pauseListener(now: float) =
  ## On EMFILE/ENFILE stop arming the level-triggered listener for a moment so
  ## the loop does not spin re-reporting an accept it cannot serve.
  if gListenPaused:
    return
  gListenPaused = true
  gListenResume = now + AcceptPause
  var ev: EpollEvent
  ev.events = 0
  ev.data.fd = gListenFd
  discard epoll_ctl(gEpfd, EPOLL_CTL_MOD, gListenFd, addr ev)

proc resumeListener() =
  if not gListenPaused:
    return
  gListenPaused = false
  var ev: EpollEvent
  ev.events = uint32(EPOLLIN)
  ev.data.fd = gListenFd
  discard epoll_ctl(gEpfd, EPOLL_CTL_MOD, gListenFd, addr ev)

# ---------------------------------------------------------------------------
# non-blocking I/O

proc flushConn(c: var Conn): FlushResult =
  while c.outOff < c.outbuf.len:
    let n = posix.send(SocketHandle(c.fd),
                       cast[pointer](addr c.outbuf[c.outOff]),
                       c.outbuf.len - c.outOff, posix.MSG_NOSIGNAL)
    if n > 0:
      c.outOff += n
    elif n == 0:
      return frPending
    else:
      let e = errno
      if e == EAGAIN or e == EWOULDBLOCK:
        return frPending
      elif e == EINTR:
        continue
      else:
        return frError
  # Fully drained: reset the buffer but keep its capacity for reuse.
  c.outbuf.setLen(0)
  c.outOff = 0
  return frDrained

proc readConn(c: var Conn): ReadResult =
  ## Drain readable bytes (edge-triggered discipline: read until EAGAIN). If
  ## unparsed input grows past MaxRequest the connection is closed as malformed;
  ## all complete requests are processed afterwards, so no read/resume state is
  ## needed and progress never depends on a new edge.
  var buf: array[ReadChunk, char]
  while true:
    let n = posix.recv(SocketHandle(c.fd), addr buf[0], ReadChunk, 0)
    if n > 0:
      let old = c.inbuf.len
      c.inbuf.setLen(old + n)
      copyMem(addr c.inbuf[old], addr buf[0], n)
      if c.inbuf.len > MaxRequest:
        return rdTooBig
    elif n == 0:
      return rdClosed        # orderly peer shutdown
    else:
      let e = errno
      if e == EAGAIN or e == EWOULDBLOCK:
        return rdOk
      elif e == EINTR:
        continue
      else:
        return rdClosed      # reset/error: flush what we can, then close
  rdOk

# ---------------------------------------------------------------------------
# request processing (transport-agnostic core is api.handleRequest)

proc processRequests(c: var Conn): bool =
  ## Parse and answer complete pipelined requests. Returns false when the framing
  ## is malformed (or non-conforming) and the connection must be closed after
  ## flushing.
  ##
  ## Every complete request present in `inbuf` is processed in one call: there is
  ## no output cap and no resume state, so processing can never stall. Input size
  ## is bounded by MaxRequest (enforced in readConn), which also bounds output.
  #
  # Advance through the buffer with a local read offset and compact the consumed
  # prefix only once at the end. Front-deleting per request was O(n^2) on a large
  # pipelined buffer (a ~1 MiB pipeline of tiny requests cost seconds of memmove).
  var pos = 0
  while true:
    let hend = c.inbuf.find("\r\n\r\n", pos)
    if hend < 0:
      # No complete header block yet: wait for more bytes unless the pending
      # header block is already oversized (never a valid request).
      if c.inbuf.len - pos > MaxHeader:
        return false
      break
    if hend - pos > MaxHeader:
      return false

    let head = parseHead(c.inbuf, pos, hend, MaxInBuf)
    if not head.ok:
      return false
    if head.bad:
      # Non-conforming framing (bad/conflicting Content-Length, Transfer-
      # Encoding, ...): answer 400 and close, rather than silently treating it
      # as CL=0 and re-parsing the body as a fresh request.
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
      # Exception covers Defect as well (OverflowDefect, IndexDefect, ...): no
      # request may ever abort the single-threaded process.
      stderr.writeLine("request handler error: " & ex.msg)
      resp = ApiResponse(status: 500, body: """{"error":"internal server error"}""")

    c.keepAlive = head.keepAlive
    let respStart = c.outbuf.len
    c.outbuf.add buildResponse(resp.status, resp.body, head.keepAlive)
    # Record responses produced while a group-commit batch is open: they must not
    # be sent until that batch commits, and become 500 if it fails to commit.
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
# per-connection event handling

proc handleEvent(fd: cint, events: uint32, now: float) =
  let idx = int(fd)
  if idx < 0 or idx >= conns.len or not conns[idx].active:
    return
  var c = addr conns[idx]
  c.lastActive = now
  var closeAfter = false   # serve buffered input, flush, then close (oversized burst)

  # Resumable output first: EPOLLOUT means the socket can take more bytes.
  if (events and uint32(EPOLLOUT)) != 0:
    if flushConn(c[]) == frError:
      closeConn(fd)
      return

  # Drain readable data when signalled (IN) or on hangup/error.
  if (events and (uint32(EPOLLIN) or uint32(EPOLLRDHUP) or
                  uint32(EPOLLHUP) or uint32(EPOLLERR))) != 0:
    case readConn(c[])
    of rdOk:
      discard
    of rdClosed:
      c.closing = true
    of rdTooBig:
      closeAfter = true      # excessive unparsed input: serve what we have, then close

  # Answer every complete buffered request, then flush. processRequests consumes
  # the entire input buffer in this one event, so there is no resume state: any
  # residual output is drained purely by the EPOLLOUT path below (an immediate
  # flush was already attempted).
  if not c.closing and c.inbuf.len > 0:
    if not processRequests(c[]):
      c.closing = true

  # Group commit: when this pass opened a write batch, the responses produced
  # against it may not be sent until it commits at the end of the pass. Withhold
  # this connection's flush and register it with `flushDeferred`. We must NOT arm
  # EPOLLOUT here (nothing was attempted), so the edge-triggered invariant
  # "EPOLLOUT is armed only after a real EAGAIN" is preserved.
  if gDb != nil and gDb.batchOpen() and c.batchCount > 0:
    if closeAfter:
      c.closing = true        # flush the batch response, then close (see flushDeferred)
    if not c.deferred:
      c.deferred = true
      gDeferredFds.add c.fd
    lruUpdate(idx)     # withheld output -> not idle -> drops out of the LRU
    return

  case flushConn(c[])
  of frError:
    closeConn(fd)
    return
  of frPending:
    c.wantWrite = true
    setInterest(c[])
  of frDrained:
    if c.wantWrite:
      c.wantWrite = false
      setInterest(c[])

  if (c.closing or closeAfter) and not c.wantWrite:
    closeConn(fd)
    return

  # Reconcile LRU membership: idle-after-this-event goes to the tail (most
  # recent), anything with buffered/pending work leaves the eviction list.
  lruUpdate(idx)

proc flushDeferred(commitOk: bool) =
  ## Release the responses withheld during this pass, after the pass's batch has
  ## been committed (or has failed). Called exactly once per pass, after
  ## `commitBatch`.
  ##
  ## On success the withheld bytes are simply flushed, exactly as the normal
  ## path would have. On a failed COMMIT every response produced against the
  ## (now rolled-back) batch is replaced by a 500: the suffix from `batchFrom`
  ## is discarded and one 500 is emitted per withheld request, so not a single
  ## success byte for an uncommitted write can reach the socket. Responses that
  ## predate the batch (already safe) are left in front of the 500s.
  for fd in gDeferredFds:
    let idx = int(fd)
    if idx < 0 or idx >= conns.len or not conns[idx].active:
      continue
    var c = addr conns[idx]
    c.deferred = false
    if not commitOk and c.batchCount > 0:
      c.outbuf.setLen(c.batchFrom)
      for _ in 0 ..< c.batchCount:
        c.outbuf.add buildResponse(500, """{"error":"internal server error"}""",
                                   c.keepAlive)
      c.closing = true
    c.batchFrom = 0
    c.batchCount = 0

    case flushConn(c[])
    of frError:
      closeConn(fd)
      continue
    of frPending:
      c.wantWrite = true
      setInterest(c[])
    of frDrained:
      if c.wantWrite:
        c.wantWrite = false
        setInterest(c[])

    if c.closing and not c.wantWrite:
      closeConn(fd)
      continue
    lruUpdate(idx)

proc doAccept(now: float) =
  while true:
    var sa: Sockaddr_in
    var slen: SockLen = SockLen(sizeof(Sockaddr_in))
    let cfd = accept4(SocketHandle(gListenFd), cast[ptr SockAddr](addr sa),
                      addr slen, SOCK_NONBLOCK or SOCK_CLOEXEC)
    if cfd == INVALID_SOCKET:
      let e = errno
      if e == EAGAIN or e == EWOULDBLOCK:
        break
      elif e == EINTR:
        continue
      elif e == EMFILE or e == ENFILE or e == ENOBUFS or e == ENOMEM:
        # Out of descriptors: reclaim idle keep-alive sockets first; only back
        # the listener off if there was nothing idle to evict. A successful
        # reclaim means we made progress, so retry the accept instead of pausing.
        let freed = evictIdleTo(max(0, openCount - EmfileBatch), now)
        if freed > 0:
          continue
        pauseListener(now)   # nothing evictable: back off, do not spin
        break
      else:
        break

    let nfd = cint(cfd)
    if int(nfd) >= conns.len:
      conns.setLen(int(nfd) + 1)
    let idx = int(nfd)
    conns[idx].fd = nfd
    conns[idx].active = true
    conns[idx].inbuf.setLen(0)
    conns[idx].outbuf.setLen(0)
    conns[idx].outOff = 0
    conns[idx].keepAlive = true
    conns[idx].lastActive = now
    conns[idx].wantWrite = false
    conns[idx].closing = false
    conns[idx].readPaused = false
    conns[idx].prev = -1
    conns[idx].next = -1
    conns[idx].inLru = false
    conns[idx].deferred = false
    conns[idx].batchFrom = 0
    conns[idx].batchCount = 0
    inc openCount

    var ev: EpollEvent
    ev.events = uint32(EPOLLIN) or uint32(EPOLLET)
    ev.data.fd = nfd
    if epoll_ctl(gEpfd, EPOLL_CTL_ADD, nfd, addr ev) != 0:
      # closeConn unregisters (DEL is a harmless ENOENT here), closes and
      # decrements openCount so accounting stays exact.
      closeConn(nfd)
      continue

    # Close the ET accept race: bytes may already be buffered before we added
    # the fd to epoll, in which case no edge will ever fire for them.
    handleEvent(nfd, uint32(EPOLLIN), now)

  # Soft high-water: only an O(1) comparison per accept; the LRU walk happens
  # solely on the rare crossing, shedding idle sockets down to the low mark.
  if openCount > gEvictHigh:
    discard evictIdleTo(gEvictLow, now)

# ---------------------------------------------------------------------------
# idle sweep

proc sweep(now: float) =
  if gListenPaused and now >= gListenResume:
    resumeListener()
  var fd = 0
  while fd < conns.len:
    if conns[fd].active:
      let pendingOut = conns[fd].wantWrite or conns[fd].outOff < conns[fd].outbuf.len
      let midReq = conns[fd].inbuf.len > 0
      let limit = if pendingOut or midReq: StallTimeout else: IdleKeepAlive
      if now - conns[fd].lastActive > limit:
        closeConn(cint(fd))
    inc fd

# ---------------------------------------------------------------------------
# setup / main loop

proc envInt(name: string, dflt: int): int =
  try:
    result = parseInt(getEnv(name, $dflt))
  except ValueError:
    result = dflt

proc raiseNofile() =
  ## Raise this process's soft RLIMIT_NOFILE up to its hard limit. This is a
  ## per-process limit only (no kernel/sysctl change) and is a no-op when the
  ## soft limit already equals the hard limit, which is the case when the
  ## server is started under `ulimit -n N`.
  var rl: RLimit
  if getrlimit(RLIMIT_NOFILE, rl) != 0:
    stderr.writeLine("getrlimit(RLIMIT_NOFILE) failed: errno=" & $errno)
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
      stderr.writeLine("setrlimit(RLIMIT_NOFILE) failed: errno=" & $errno &
                       " (soft=" & $soft0 & " hard=" & $hard & ")")
  else:
    stderr.writeLine("RLIMIT_NOFILE soft==hard==" & $soft0 & " (nothing to raise)")

proc setupListener(cfg: Config): bool =
  gListenFd = cint(posix.socket(posix.AF_INET,
                                posix.SOCK_STREAM or SOCK_NONBLOCK, 0))
  if gListenFd < 0:
    stderr.writeLine("socket() failed: " & $errno)
    return false

  var one: cint = 1
  discard posix.setsockopt(SocketHandle(gListenFd), posix.SOL_SOCKET,
                           posix.SO_REUSEADDR, addr one, SockLen(sizeof(one)))

  # TCP_DEFER_ACCEPT: the kernel does not make a completed handshake acceptable
  # until the client sends its first byte (or this many seconds pass). HTTP
  # clients send immediately, so accept still fires at once, while a connection
  # that handshakes and stays silent never consumes an accept wakeup. Applies to
  # the listen socket; TCP_DEFER_ACCEPT_SEC=0 disables it.
  let deferSec = envInt("TCP_DEFER_ACCEPT_SEC", 1)
  if deferSec > 0:
    var dv: cint = cint(deferSec)
    discard posix.setsockopt(SocketHandle(gListenFd), posix.IPPROTO_TCP,
                             TCP_DEFER_ACCEPT, addr dv, SockLen(sizeof(dv)))

  var sa: Sockaddr_in
  sa.sin_family = TSa_Family(posix.AF_INET)
  sa.sin_port = nativesockets.htons(uint16(cfg.port))
  if posix.inet_pton(posix.AF_INET, cfg.host.cstring, addr sa.sin_addr) != 1:
    stderr.writeLine("invalid HOST: " & cfg.host)
    return false

  if posix.bindSocket(SocketHandle(gListenFd), cast[ptr SockAddr](addr sa),
                      SockLen(sizeof(sa))) != 0:
    stderr.writeLine("bind() failed on " & cfg.host & ":" & $cfg.port &
                     ": " & $errno)
    return false

  if posix.listen(SocketHandle(gListenFd), cint(ListenBacklog)) != 0:
    stderr.writeLine("listen() failed: " & $errno)
    return false

  gEpfd = epoll_create1(EPOLL_CLOEXEC)
  if gEpfd < 0:
    stderr.writeLine("epoll_create1() failed: " & $errno)
    return false

  var ev: EpollEvent
  ev.events = uint32(EPOLLIN)
  ev.data.fd = gListenFd
  if epoll_ctl(gEpfd, EPOLL_CTL_ADD, gListenFd, addr ev) != 0:
    stderr.writeLine("epoll_ctl(ADD listen) failed: " & $errno)
    return false
  return true

proc applyEvictionPolicy() =
  ## Eviction water marks (hysteresis): keep LOW strictly below HIGH so an
  ## eviction pass always makes progress.
  gEvictHigh = envInt("EVICT_HIGH", EvictHighDefault)
  gEvictLow = envInt("EVICT_LOW", EvictLowDefault)
  if gEvictLow >= gEvictHigh:
    gEvictLow = gEvictHigh * 9 div 10
  stderr.writeLine("eviction policy: high=" & $gEvictHigh &
                   " low=" & $gEvictLow & " emfileBatch=" & $EmfileBatch)

proc runLoop() =
  ## The one epoll event loop.
  conns = newSeq[Conn](64)
  var events: array[MaxEvents, EpollEvent]

  while true:
    let n = epoll_wait(gEpfd, addr events[0], cint(MaxEvents), cint(SweepMs))
    if n < 0:
      if errno == EINTR:
        continue
      stderr.writeLine("epoll_wait() failed: " & $errno)
      quit(1)

    let now = epochTime()
    for i in 0 ..< int(n):
      let fd = events[i].data.fd
      if fd == gListenFd:
        if not gListenPaused:
          doAccept(now)
      else:
        handleEvent(fd, events[i].events, now)

    # End of one epoll_wait pass: commit the pass's write batch (if any) and only
    # then release the responses withheld for it (rule 6: durable before respond).
    # A read-only pass never opened a batch, so gDeferredFds is empty and this is
    # a no-op -- read latency is unchanged.
    if gDb != nil and gDb.batchOpen():
      flushDeferred(gDb.commitBatch())
    elif gDeferredFds.len > 0:
      # Defensive: a withheld connection can only exist inside an open batch.
      flushDeferred(true)

    sweep(now)

proc main() =
  let cfg = loadConfig()

  # ---------------------------------------------------------------------------
  # Single process: one EXCLUSIVE sqlite connection opened first, SO_REUSEADDR
  # only, then setupListener and one loop.
  # ---------------------------------------------------------------------------
  gDb = openDb(cfg.sqlitePath)
  # Write batching across one epoll pass; GROUP_COMMIT=0 restores autocommit.
  gDb.setGroupCommit(envInt("GROUP_COMMIT", 1) != 0)
  gSecret = cfg.jwtSecret
  markStart()

  discard posix.signal(SIGPIPE, SIG_IGN)

  # Raise our own fd ceiling (per-process; no kernel tuning) before any fds.
  raiseNofile()

  applyEvictionPolicy()

  if not setupListener(cfg):
    quit(1)

  runLoop()

main()
