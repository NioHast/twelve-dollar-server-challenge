## Byte-level JSON writing and minimal request-body parsing.
##
## M1 note: parseBody uses std/json for correctness. M5 replaces it with a
## hand-rolled zero-allocation parser; the interface stays identical.
import std/json
import model

const HexDigits = "0123456789abcdef"

proc addEscaped*(buf: var string, s: string) =
  ## Append `s` as the *contents* of a JSON string (no surrounding quotes).
  for c in s:
    case c
    of '"':  buf.add "\\\""
    of '\\': buf.add "\\\\"
    of '\b': buf.add "\\b"
    of '\f': buf.add "\\f"
    of '\n': buf.add "\\n"
    of '\r': buf.add "\\r"
    of '\t': buf.add "\\t"
    else:
      let o = c.ord
      if o < 0x20:
        buf.add "\\u00"
        buf.add HexDigits[(o shr 4) and 0xF]
        buf.add HexDigits[o and 0xF]
      else:
        buf.add c

proc addJStr*(buf: var string, s: string) =
  buf.add '"'
  buf.addEscaped s
  buf.add '"'

proc addJInt*(buf: var string, v: int64) =
  buf.add $v

proc writePost*(buf: var string, p: Post) =
  buf.add "{\"id\":"
  buf.addJInt p.id
  buf.add ",\"body\":"
  buf.addJStr p.body
  buf.add ",\"created_at\":"
  buf.addJStr p.createdAt
  buf.add ",\"author\":"
  buf.addJStr p.author
  buf.add ",\"like_count\":"
  buf.addJInt p.likeCount
  buf.add "}"

proc writeFeed*(buf: var string, posts: openArray[Post]) =
  buf.add "{\"posts\":["
  for i in 0 ..< posts.len:
    if i > 0: buf.add ','
    buf.writePost posts[i]
  buf.add "]}"

type BodyKind* = enum
  bkOk, bkMalformed, bkRequired

proc parseBody*(raw: string): tuple[kind: BodyKind, body: string] =
  ## bkMalformed -> invalid JSON; bkRequired -> no usable string "body"; bkOk -> decoded body (raw, untrimmed).
  var node: JsonNode
  try:
    node = parseJson(raw)
  except CatchableError:
    return (bkMalformed, "")
  if node.kind != JObject or not node.hasKey("body"):
    return (bkRequired, "")
  let b = node["body"]
  if b.kind != JString:
    return (bkRequired, "")
  (bkOk, b.getStr)
