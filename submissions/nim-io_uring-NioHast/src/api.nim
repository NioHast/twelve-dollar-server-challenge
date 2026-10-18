## Transport-agnostic request handling: routing, auth ordering, exact error semantics.
## Implements the contract in SPEC.md / test/test.sh byte-for-byte.
import std/[strutils, times]
import db, jwt, jsonbuf, http

type ApiResponse* = object
  status*: int
  body*: string
  ## The server writes this response's body directly into the connection output
  ## buffer (`db.writeFeedResponse`) because the body is large and would
  ## otherwise be built as a throwaway string and copied. `body` is empty when
  ## this is set. Every other endpoint leaves it false and uses `body`.
  feedDirect*: bool

const
  ErrNotFound     = """{"error":"not found"}"""
  ErrInvalidId    = """{"error":"invalid post id"}"""
  ErrPostNotFound = """{"error":"post not found"}"""
  ErrMissingBearer = """{"error":"missing bearer token"}"""
  ErrInvalidToken  = """{"error":"invalid or expired token"}"""
  ErrBadPayload    = """{"error":"invalid token payload"}"""
  ErrMalformed     = """{"error":"malformed JSON body"}"""
  ErrBodyRequired  = """{"error":"body is required"}"""
  ErrTooLong       = """{"error":"body must be at most 500 characters"}"""
  ErrInternal      = """{"error":"internal server error"}"""

# --- uptime support (shared across threads) ---
var gStartTime {.global.}: float
proc markStart*() = gStartTime = epochTime()
proc uptimeSeconds*(): int64 =
  let now = epochTime()
  int64(now - gStartTime)

proc jsonRes(status: int, body: string): ApiResponse =
  ApiResponse(status: status, body: body)

proc parsePositiveId(buf: openArray[char]; s: Span): tuple[ok: bool, id: int64] =
  ## Accept only ASCII digits forming a value >= 1 that fits in int64.
  ##
  ## Overflow guard: int64 holds at most 19 decimal digits, and the check below
  ## bounds `v` against the *next* digit before multiplying, so no intermediate
  ## `v*10 + d` can overflow. The old guard (`v > 922337203685477580`) fired one
  ## step too late: v=922337203685477580 with d=8 or 9 overflowed and raised a
  ## Defect that aborted the single-threaded process (both fixes below: the
  ## `len > 19` cap and the digit-aware bound).
  if s.len == 0 or s.len > 19: return (false, 0'i64)
  var v: int64 = 0
  for i in 0 ..< s.len:
    let c = buf[s.off + i]
    if c < '0' or c > '9': return (false, 0'i64)
    let d = int64(ord(c) - ord('0'))
    if v > (9_223_372_036_854_775_807'i64 - d) div 10:
      return (false, 0'i64)  # v*10 + d would overflow int64
    v = v * 10 + d
  if v < 1: return (false, 0'i64)
  (true, v)

type
  AuthKind = enum akMissing, akInvalid, akBadPayload, akOk
  Auth = object
    kind: AuthKind
    userId: int64
    username: string

proc doAuth(buf: openArray[char]; authHeader: Span; secret: string): Auth =
  if not spanStartsWith(buf, authHeader, "Bearer "):
    return Auth(kind: akMissing)
  let token = subSpan(authHeader, 7, authHeader.len)
  let (status, info) = verifyJwt(buf, token, secret)
  case status
  of jsOk:      Auth(kind: akOk, userId: info.userId, username: info.username)
  of jsInvalid: Auth(kind: akInvalid)
  of jsBadPayload: Auth(kind: akBadPayload)

proc authResponse(a: Auth): ApiResponse =
  case a.kind
  of akMissing:    jsonRes(401, ErrMissingBearer)
  of akInvalid:    jsonRes(401, ErrInvalidToken)
  of akBadPayload: jsonRes(401, ErrBadPayload)
  of akOk:         ApiResponse(status: 0, body: "")  # sentinel; never used

proc handleRequest*(database: Db; jwtSecret: string; reqBuf: openArray[char];
                    httpMethod, path, authHeader: Span;
                    rawBody: string): ApiResponse =
  if spanEq(reqBuf, httpMethod, "GET"):
    if spanEq(reqBuf, path, "/health"):
      if database.health():
        return jsonRes(200, """{"status":"ok","db":"ok","uptime_s":""" & $uptimeSeconds() & "}")
      else:
        return jsonRes(503, """{"status":"degraded","db":"unreachable","error":"db unreachable"}""")
    if spanEq(reqBuf, path, "/feed"):
      # The transport writes the feed body straight into the pooled response
      # buffer; no intermediate body string is built here.
      return ApiResponse(status: 200, feedDirect: true)
    if spanStartsWith(reqBuf, path, "/posts/"):
      let pid = parsePositiveId(reqBuf, subSpan(path, 7, path.len))
      if not pid.ok: return jsonRes(400, ErrInvalidId)
      # The DB writes the complete `{"post":...}` body straight from the row
      # columns (no Post object, column strings or re-escaping); `body` stays
      # empty on a miss, so the 404 bytes are unchanged.
      var body = ""
      if database.writePostJson(pid.id, body):
        return jsonRes(200, body)
      else:
        return jsonRes(404, ErrPostNotFound)
    return jsonRes(404, ErrNotFound)

  if spanEq(reqBuf, httpMethod, "POST"):
    if spanEq(reqBuf, path, "/posts"):
      let a = doAuth(reqBuf, authHeader, jwtSecret)          # auth BEFORE body validation
      if a.kind != akOk: return authResponse(a)
      let pb = parseBody(rawBody)
      case pb.kind
      of bkMalformed: return jsonRes(400, ErrMalformed)
      of bkRequired:  return jsonRes(400, ErrBodyRequired)
      of bkOk:
        let trimmed = pb.body.strip
        if trimmed.len == 0: return jsonRes(400, ErrBodyRequired)
        if trimmed.len > 500: return jsonRes(400, ErrTooLong)
        # The DB writes the complete `{"post":...}` body straight from the
        # inserted row's values; no Post object or second escaping pass.
        var body = ""
        if database.createPostResponseBody(a.userId, a.username, trimmed, body):
          return jsonRes(201, body)
        else:
          return jsonRes(500, ErrInternal)
    if spanStartsWith(reqBuf, path, "/posts/") and spanEndsWith(reqBuf, path, "/like"):
      let a = doAuth(reqBuf, authHeader, jwtSecret)          # auth BEFORE id validation
      if a.kind != akOk: return authResponse(a)
      # "/posts/like" satisfies both the "/posts/" prefix and the "/like"
      # suffix, so the id slice `path[7 ..< path.len - 5]` has its end before
      # its start. The frozen baseline raises RangeDefect there, which the
      # request loop maps to 500; preserve that exact wire behavior without
      # constructing a negative-length span.
      let midEnd = path.len - 5
      if midEnd < 7:
        return jsonRes(500, ErrInternal)
      let mid = subSpan(path, 7, midEnd)
      let pid = parsePositiveId(reqBuf, mid)
      if not pid.ok: return jsonRes(400, ErrInvalidId)
      case database.like(a.userId, pid.id)
      of loLiked:
        return jsonRes(201, """{"liked":true,"already_liked":false,"post_id":""" & $pid.id & "}")
      of loAlready:
        return jsonRes(200, """{"liked":true,"already_liked":true,"post_id":""" & $pid.id & "}")
      of loNoPost:
        return jsonRes(404, ErrPostNotFound)
    return jsonRes(404, ErrNotFound)

  jsonRes(404, ErrNotFound)
