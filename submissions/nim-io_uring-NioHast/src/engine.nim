## engine.nim: transport-agnostic server scaffold shared by both transports.
##
## Everything here is byte-for-byte identical between the default io_uring
## transport (`server_iouring.nim`) and the epoll fallback (`server_epoll.nim`): the
## per-process fd-limit bump, the listening-socket setup, and the HTTP
## request/response loop (`processRequests`). Keeping a single copy is what stops
## the two transports from drifting - the same reason `http.nim` owns the parser
## and the response framing. Nothing here touches an event loop or a connection
## struct; each transport adapts its own `Conn` to these routines.

import std/os
import std/oserrors
import std/posix
import std/[nativesockets, strutils]

import db, api, http

# ---------------------------------------------------------------------------
# syscall surface std/posix does not expose (or that we want verbatim)

var SOCK_NONBLOCK {.importc, header: "<sys/socket.h>".}: cint
# TCP_DEFER_ACCEPT (value in seconds) is in <netinet/tcp.h>.
var TCP_DEFER_ACCEPT {.importc, header: "<netinet/tcp.h>".}: cint
# Per-connection TCP options (applied to an accepted socket; see applyTcpTuning).
var TCP_QUICKACK {.importc, header: "<netinet/tcp.h>".}: cint
var TCP_NOTSENT_LOWAT {.importc, header: "<netinet/tcp.h>".}: cint

proc envIntDef(name: string, dflt: int): int =
  try:
    parseInt(getEnv(name, $dflt))
  except ValueError:
    dflt

proc tcpNotsentLowatBytes*(): int =
  ## TCP_NOTSENT_LOWAT value in bytes; 0 disables it (the default).
  result = envIntDef("TCP_NOTSENT_LOWAT_BYTES", 0)
  if result < 0: result = 0

proc tcpQuickAck*(): bool =
  ## Whether to set TCP_QUICKACK on accepted sockets (default off).
  getEnv("TCP_QUICKACK", "0") == "1"

proc applyTcpTuning*(fd: cint) =
  ## Optional, env-gated per-connection TCP tuning (both default OFF, so the
  ## default build is unchanged):
  ##   * TCP_NOTSENT_LOWAT_BYTES > 0 caps the unsent write queue (lower kernel
  ##     memory per socket and a shorter socket-lock hold under backlog);
  ##   * TCP_QUICKACK=1 disables delayed ACKs (lower latency, more ACK traffic).
  if fd < 0: return
  let lowat = tcpNotsentLowatBytes()
  if lowat > 0:
    var v: cint = cint(lowat)
    discard posix.setsockopt(SocketHandle(fd), posix.IPPROTO_TCP,
                             TCP_NOTSENT_LOWAT, addr v, SockLen(sizeof(v)))
  if tcpQuickAck():
    var one: cint = 1
    discard posix.setsockopt(SocketHandle(fd), posix.IPPROTO_TCP,
                             TCP_QUICKACK, addr one, SockLen(sizeof(one)))

# ---------------------------------------------------------------------------
# setup helpers

proc raiseNofile*() =
  ## Raise this process's soft RLIMIT_NOFILE up to its hard limit. Per-process
  ## only (no kernel/sysctl change), and a no-op when the soft limit already
  ## equals the hard limit, which is the case when started under `ulimit -n N`.
  var rl: RLimit
  if getrlimit(RLIMIT_NOFILE, rl) != 0:
    stderr.writeLine("getrlimit(RLIMIT_NOFILE) failed: errno=" &
                     $(osLastError().int))
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
      stderr.writeLine("setrlimit(RLIMIT_NOFILE) failed: errno=" &
                       $(osLastError().int) & " (soft=" & $soft0 &
                       " hard=" & $hard & ")")
  else:
    stderr.writeLine("RLIMIT_NOFILE soft==hard==" & $soft0 & " (nothing to raise)")

proc setupListenSocket*(host: string; port, backlog, deferSec: int): cint =
  ## Create, bind and listen a non-blocking IPv4 socket, returning its fd (or -1,
  ## after logging, on any failure). `deferSec > 0` enables TCP_DEFER_ACCEPT
  ## before `listen`: the kernel withholds a completed handshake from `accept`
  ## until the client sends its first byte (HTTP clients send at once, so accept
  ## still fires immediately, but a silent handshake never wakes the loop). The
  ## io_uring transport passes 0 and keeps its previous behaviour.
  result = cint(posix.socket(posix.AF_INET,
                             posix.SOCK_STREAM or SOCK_NONBLOCK, 0))
  if result < 0:
    stderr.writeLine("socket() failed: " & $(osLastError().int))
    return -1
  var one: cint = 1
  discard posix.setsockopt(SocketHandle(result), posix.SOL_SOCKET,
                           posix.SO_REUSEADDR, addr one, SockLen(sizeof(one)))
  if deferSec > 0:
    var dv: cint = cint(deferSec)
    discard posix.setsockopt(SocketHandle(result), posix.IPPROTO_TCP,
                             TCP_DEFER_ACCEPT, addr dv, SockLen(sizeof(dv)))
  var sa: Sockaddr_in
  sa.sin_family = TSa_Family(posix.AF_INET)
  sa.sin_port = nativesockets.htons(uint16(port))
  if posix.inet_pton(posix.AF_INET, host.cstring, addr sa.sin_addr) != 1:
    stderr.writeLine("invalid HOST: " & host)
    discard posix.close(result)
    return -1
  if posix.bindSocket(SocketHandle(result), cast[ptr SockAddr](addr sa),
                      SockLen(sizeof(sa))) != 0:
    stderr.writeLine("bind() failed on " & host & ":" & $port & ": " &
                     $(osLastError().int))
    discard posix.close(result)
    return -1
  if posix.listen(SocketHandle(result), cint(backlog)) != 0:
    stderr.writeLine("listen() failed: " & $(osLastError().int))
    discard posix.close(result)
    return -1

# ---------------------------------------------------------------------------
# request processing (the transport-agnostic core is api.handleRequest)

proc processRequests*(inbuf, outbuf: var string; keepAlive, closing: var bool;
                      outOff: int; batchFrom, batchCount: var int;
                      database: Db; secret: string;
                      maxInBuf, maxOutBuf: int): bool =
  ## Parse and answer every complete pipelined request currently in `inbuf`,
  ## appending the response bytes to `outbuf`. Returns false when the framing is
  ## malformed (or non-conforming) and the connection must close after flushing.
  ##
  ## `maxOutBuf > 0` stops the loop once that many response bytes are buffered
  ## (the io_uring path), leaving the remainder for the send-drain to finish; 0
  ## means no cap (the epoll path drains everything buffered in one pass).
  #
  # Advance with a local offset and compact once at the end; front-deleting per
  # request was O(n^2) on a large pipelined buffer.
  var pos = 0
  while true:
    if maxOutBuf > 0 and outbuf.len - outOff >= maxOutBuf:
      break
    let hend = inbuf.find("\r\n\r\n", pos)
    if hend < 0:
      if inbuf.len - pos > MaxHeader:
        return false
      break
    if hend - pos > MaxHeader:
      return false

    let head = parseHead(inbuf, pos, hend, maxInBuf)
    if not head.ok:
      return false
    if head.bad:
      # Non-conforming framing (bad/conflicting Content-Length, Transfer-
      # Encoding, ...): answer 400 and close, rather than silently treating it
      # as CL=0 and re-parsing the body as a fresh request.
      outbuf.add buildResponse(400, """{"error":"bad request"}""", false)
      closing = true
      return false
    if head.contentLen > maxInBuf:
      return false

    let totalLen = (hend - pos) + 4 + head.contentLen
    if inbuf.len - pos < totalLen:
      break                  # body not fully arrived
    var rawBody = ""
    if head.contentLen > 0:
      let b = hend + 4
      rawBody = inbuf[b ..< b + head.contentLen]
    pos += totalLen

    var resp: ApiResponse
    try:
      resp = handleRequest(database, secret, head.httpMethod, head.path,
                           head.auth, rawBody)
    except Exception as ex:
      # Exception covers Defect as well (OverflowDefect, IndexDefect, ...): no
      # request may ever abort the single-threaded process.
      stderr.writeLine("request handler error: " & ex.msg)
      resp = ApiResponse(status: 500, body: """{"error":"internal server error"}""")

    keepAlive = head.keepAlive
    let respStart = outbuf.len
    outbuf.add buildResponse(resp.status, resp.body, head.keepAlive)
    # Record responses produced while a group-commit batch is open: they must not
    # be sent until that batch commits, and become 500 if it fails to commit.
    if database != nil and database.batchOpen():
      if batchCount == 0:
        batchFrom = respStart
      inc batchCount
    if not head.keepAlive:
      closing = true
      break

  if pos > 0:
    inbuf.delete(0 ..< pos)
  true
