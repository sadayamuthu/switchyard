# switchyard v0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go library (plus a debug binary) that proxies MCP over stdio transparently and lets consumers hook requests and responses by method.

**Architecture:** switchyard frames newline-delimited JSON-RPC in both directions and forwards every message as its original bytes. Only client→server requests pass through hooks; everything else (notifications, server→client requests, their responses, unknown shapes) is forwarded untouched. Hooks return a `Decision`; responses to hooked requests are passed back through hooks in reverse order. The official Go MCP SDK is used only in `testkit` (real client and server for tests), never in the proxy path.

**Tech Stack:** Go 1.26, standard library, `github.com/modelcontextprotocol/go-sdk` v1.8.0 (testkit only).

**Spec:** `~/git-openastra/agentchaos/docs/superpowers/specs/2026-11-22-switchyard-agentchaos-v0-design.md`

## Global Constraints

- Module path `github.com/sadayamuthu/switchyard`; `go 1.26`.
- No tool-specific logic in switchyard (no faults, policy, counting).
- Transparent by default: with no hooks, bytes out equal bytes in, per message.
- No model in the loop.
- stdio only in v0; no Streamable HTTP.
- macOS and Linux.
- Event schema versioned from the first release: field `"v": 1`.
- Chain ID env var: `SWITCHYARD_CHAIN_ID`; first hop generates it if absent.
- Repo is private; nothing in this plan publishes a release.

## Review Focus

1. **Large messages** (tool results of several MB): must pass intact; a default `bufio.Scanner` buffer silently breaks at 64 KB. Test in Task 1.
2. **Upstream exits mid-call:** every pending downstream request gets a JSON-RPC error (`-32603`, message `upstream closed`) and `Serve` returns; nothing hangs. Test in Task 2.
3. **Client cancels a hung or slowed call:** the hook's context is cancelled and the pending entry is removed (no leak). Test in Task 3.
4. **A hook panics:** the client gets a JSON-RPC error (`-32603`, `hook <name> panicked`) for that request; the proxy keeps serving others. Test in Task 3.
5. **JSON-RPC batch arrays** (older MCP revisions) and non-JSON lines: forwarded untouched, never intercepted, never crash. Test in Task 1/2.

---

## File Structure

```
switchyard/
  go.mod
  jsonrpc/message.go        Message type, Kind, parse preserving raw bytes
  jsonrpc/codec.go          line Reader / mutex-guarded Writer
  jsonrpc/codec_test.go
  hook.go                   Hook, Base, Request, Response, Decision constructors
  tool.go                   typed helpers for tools/call and tools/list
  session.go                Session: ID, ChainID, KV store, Tools
  proxy.go                  Proxy, Options, Serve
  pending.go                pending-request table, cancellation
  events.go                 Event, EventSink, JSONLSink, chain ID
  command.go                RunCommand: spawn upstream, wire stdio, env
  proxy_test.go, hooks_test.go, response_test.go, events_test.go
  testkit/testkit.go        Server, ToolSpec, Connect, Recorder
  internal/testserver/main.go   stdio MCP server binary for e2e tests
  cmd/switchyard/main.go    pass-through debug proxy
  e2e_test.go               chain of two real proxy processes
```

---

### Task 1: Module and JSON-RPC codec

**Files:**
- Create: `go.mod`, `jsonrpc/message.go`, `jsonrpc/codec.go`
- Test: `jsonrpc/codec_test.go`

**Interfaces:**
- Produces:
  - `type Kind int` with `KindRequest, KindNotification, KindResponse, KindOther` (`KindOther` = batch array, non-object, or invalid JSON).
  - `type Error struct { Code int \`json:"code"\`; Message string \`json:"message"\`; Data json.RawMessage \`json:"data,omitempty"\` }`
  - `type Message struct { ID json.RawMessage; Method string; Params, Result json.RawMessage; Error *Error; Raw []byte }` — `Raw` holds the original line without the trailing newline.
  - `func Parse(line []byte) *Message` — never errors; unparseable input yields a `Message` whose `Kind()` is `KindOther`.
  - `func (m *Message) Kind() Kind`; `func (m *Message) IDKey() string` (the raw ID bytes as a string, for map keys).
  - `func NewResponse(id json.RawMessage, result json.RawMessage) *Message`, `func NewErrorResponse(id json.RawMessage, e *Error) *Message` — both fill `Raw`.
  - `type Reader struct`, `func NewReader(r io.Reader) *Reader`, `func (r *Reader) Read() (*Message, error)` — returns `io.EOF` at end; skips empty lines.
  - `type Writer struct`, `func NewWriter(w io.Writer) *Writer`, `func (w *Writer) Write(m *Message) error` — writes `m.Raw` + `\n` under a mutex.

- [ ] **Step 1: `go mod init github.com/sadayamuthu/switchyard`, set `go 1.26`.**
- [ ] **Step 2: Write failing tests**
  - `TestParseKinds`: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}` → Request; no `id` → Notification; `{"jsonrpc":"2.0","id":1,"result":{}}` → Response; `[{"jsonrpc":"2.0","id":1,"method":"x"}]` → Other; `not json` → Other.
  - `TestRawPreserved`: line with unusual key order and whitespace `{"method":"x", "jsonrpc":"2.0","id":"a"}` round-trips through Reader→Writer byte-identical.
  - `TestLargeMessage`: a 5 MB response line reads intact.
  - `TestIDKeyDistinguishesTypes`: `IDKey` of id `1` ≠ id `"1"`.
- [ ] **Step 3: Run `go test ./jsonrpc/...` — expect FAIL (undefined).**
- [ ] **Step 4: Implement.** Reader uses `bufio.Reader.ReadBytes('\n')` (no line cap), strips `\r\n`/`\n`.
- [ ] **Step 5: Run `go test ./jsonrpc/...` — expect PASS.**
- [ ] **Step 6: Commit** `feat: JSON-RPC message codec preserving raw bytes`

---

### Task 2: Transparent pass-through proxy and testkit

**Files:**
- Create: `session.go` (ID and ChainID only for now), `proxy.go`, `pending.go`, `testkit/testkit.go`
- Test: `proxy_test.go`

**Interfaces:**
- Consumes: `jsonrpc` package (Task 1).
- Produces:
  - `type Options struct { Name string; Hooks []Hook; Sink EventSink; ChainID string }` (`Hook`/`EventSink` declared as empty interfaces in this task's files, filled in Tasks 3 and 5).
  - `func New(opts Options) *Proxy`
  - `func (p *Proxy) Serve(ctx context.Context, down io.Reader, downOut io.Writer, up io.Reader, upOut io.Writer) error` — `down`: client→proxy, `downOut`: proxy→client, `up`: server→proxy (server stdout), `upOut`: proxy→server (server stdin). Returns `nil` when `down` hits EOF; returns after closing `upOut` (if it is an `io.Closer`).
  - `testkit.ToolSpec{ Name, Description string; Handler func(ctx context.Context, args map[string]any) (*mcp.CallToolResult, error) }`
  - `testkit.Server(t testing.TB, tools ...ToolSpec) *mcp.Server`
  - `testkit.Connect(t testing.TB, server *mcp.Server, opts switchyard.Options) *mcp.ClientSession` — wires SDK client ↔ proxy ↔ SDK server with `io.Pipe`s and `mcp.IOTransport`; cleanup via `t.Cleanup`.
  - `testkit.ConnectDirect(t, server) *mcp.ClientSession` — same without the proxy, for comparison.

- [ ] **Step 1: Write failing tests** (all via testkit, no hooks):
  - `TestPassThroughInitialize`: `InitializeResult()` server info and capabilities via proxy equal those via `ConnectDirect`.
  - `TestPassThroughToolsListAndCall`: list returns tool `echo`; `CallTool(echo, {"text":"hi"})` returns text `hi`.
  - `TestPassThroughServerToClient`: server tool handler sends a logging notification to the client during the call; client receives it.
  - `TestPassThroughByteExact`: drive `Serve` directly with raw lines (a request with odd key order, a batch array, a non-JSON line, a notification); bytes written to `upOut` equal input lines exactly.
  - `TestUpstreamClosesMidCall`: tool handler blocks; close upstream pipes; client's `CallTool` returns an error containing `upstream closed`; `Serve` returns.
- [ ] **Step 2: Run `go test ./...` — expect FAIL.**
- [ ] **Step 3: Implement.** Two loops: downstream reader (requests recorded in pending table keyed by `IDKey`, then forwarded) and upstream reader (responses whose ID is pending are removed and forwarded; everything else forwarded as-is). Server→client requests and client responses to them are never added to pending. On upstream EOF, answer every pending request with `NewErrorResponse(id, &Error{Code:-32603, Message:"upstream closed"})`.
- [ ] **Step 4: Run `go test -race ./...` — expect PASS.**
- [ ] **Step 5: Commit** `feat: transparent stdio pass-through proxy and testkit`

---

### Task 3: Request hooks and decisions

**Files:**
- Create: `hook.go`, `tool.go` (request side)
- Modify: `proxy.go`, `pending.go`
- Test: `hooks_test.go`

**Interfaces:**
- Consumes: Task 2.
- Produces:
  - `type Request struct { ID json.RawMessage; Method string; Params json.RawMessage }`
  - `type Response struct { Result json.RawMessage; Error *jsonrpc.Error }`
  - `type Hook interface { Name() string; OnRequest(ctx context.Context, s *Session, r *Request) Decision; OnResponse(ctx context.Context, s *Session, r *Request, resp *Response) *Response }`
  - `type Base struct{}` — embeddable no-op `OnRequest` (returns `Continue()`) and `OnResponse` (returns `resp`). Consumers set `Name()` themselves.
  - `type Decision struct { /* unexported kind + payload; must be able to gain Hold later */ }` and constructors:
    `Continue()`, `ContinueWith(params json.RawMessage)`, `Block(e *jsonrpc.Error)`, `BlockTool(message string)` (returns a `tools/call` result `{"content":[{"type":"text","text":message}],"isError":true}`), `Respond(result json.RawMessage)`, `Hang()`.
    `func (d Decision) String() string` → `continue`, `continue-with`, `block`, `block-tool`, `respond`, `hang` (used in events).
  - `type ToolCall struct { Name string; Arguments json.RawMessage }`; `func (r *Request) ToolCall() (*ToolCall, bool)` (true only for `tools/call`).

**Rules (spec-pinned, implement exactly):**
- Hooks run in `Options.Hooks` order. `ContinueWith` replaces params and evaluation continues with the next hook. `Block`, `BlockTool`, `Respond`, `Hang` stop evaluation and the request is never sent upstream.
- `tools/call` requests are hooked on their own goroutine (a slow or hung call must not block other traffic). All other requests are hooked synchronously in the read loop, preserving order (e.g. `initialize` before `notifications/initialized`).
- Each hooked request gets a context cancelled when the client sends `notifications/cancelled` for its ID (the notification is still forwarded upstream), or when the session ends.
- `Hang`: no response is ever written; the pending entry is removed on cancel or session end.
- A panic in a hook → `Block(&jsonrpc.Error{Code:-32603, Message:"hook <name> panicked"})` for that request.

- [ ] **Step 1: Write failing tests** (testkit server with tools `echo` and `slow`):
  - `TestContinueWithRewritesArgs`: hook rewrites `{"text":"hi"}` → `{"text":"bye"}`; result text `bye`.
  - `TestBlockJSONRPC`: `Block(&jsonrpc.Error{Code:-32001, Message:"denied"})`; `CallTool` errors containing `denied`; upstream handler never called (counter = 0).
  - `TestBlockTool`: result `IsError == true`, text equals the message.
  - `TestRespondSynthesized`: hook returns `Respond(`{"content":[{"type":"text","text":"fake"}]}`)`; result text `fake`; upstream not called.
  - `TestFirstTerminalDecisionWins`: hooks [ContinueWith, Block, Respond] → blocked; third hook's `OnRequest` not called.
  - `TestSlowCallDoesNotBlockOthers`: hook sleeps 500 ms for `slow` only; concurrent `echo` returns first.
  - `TestHangThenCancel`: hook returns `Hang`; call with a 100 ms context; `CallTool` returns context error; within 1 s the proxy's pending count is 0 (unexported `p.pendingLen()` accessor in package test).
  - `TestHookPanic`: hook panics on `echo`; call errors with `hook boom panicked`; a following call (hook not panicking) succeeds.
  - `TestNonToolRequestsOrdered`: hook that records methods sees `initialize` before any later request; client initializes successfully.
- [ ] **Step 2: Run `go test -race ./...` — expect FAIL.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run `go test -race ./...` — expect PASS.**
- [ ] **Step 5: Commit** `feat: request hooks with continue/rewrite/block/respond/hang`

---

### Task 4: Response hooks, sessions, typed helpers

**Files:**
- Modify: `session.go`, `tool.go`, `proxy.go`
- Test: `response_test.go`

**Interfaces:**
- Consumes: Task 3.
- Produces:
  - `func (s *Session) ID() string`, `ChainID() string`, `Get(key string) (any, bool)`, `Set(key string, v any)`, `Update(key string, fn func(old any) any) any` (atomic read-modify-write, for counters), `Tools() []ToolInfo` (from the most recent `tools/list` response, after hooks).
  - `type ToolInfo struct { Name string; Raw json.RawMessage }`
  - `type ToolResult struct { Content []json.RawMessage \`json:"content"\`; IsError bool \`json:"isError,omitempty"\`; Rest map[string]json.RawMessage \`json:"-"\` }` — `Rest` keeps unknown fields so round-trips don't drop them.
  - `func (r *Response) ToolResult() (*ToolResult, bool)`; `func (tr *ToolResult) Marshal() json.RawMessage`
  - `func (r *Response) ToolList() ([]ToolInfo, bool)`; `func ToolListResult(orig json.RawMessage, tools []ToolInfo) json.RawMessage` (replaces only the `tools` field).

**Rules:** `OnResponse` runs for every hook in reverse order, only for requests that were forwarded upstream (not for Block/Respond/Hang). Each hook receives the previous hook's output. If no hook changed the response (pointer-equal), forward the original raw bytes. One `Session` per `Serve` call; `ID()` is a random 16-hex string.

- [ ] **Step 1: Write failing tests:**
  - `TestOnResponseReverseOrder`: hooks A, B append their name to the result text; final text `echo:BA`.
  - `TestToolListRewrite`: hook removes tool `secret` from `tools/list`; client sees only `echo`; `Session.Tools()` excludes `secret`.
  - `TestObserveResourcesRead`: testkit server with resource `file:///a` containing `hello`; hook records `resources/read` result text into Session; assert recorded `hello` and client result unchanged.
  - `TestToolResultRoundTripKeepsUnknownFields`: result with `structuredContent` survives `ToolResult()` → `Marshal()`.
  - `TestSessionUpdateConcurrent`: 100 concurrent `Update("n", +1)` → 100.
  - `TestUnchangedResponseBytesIdentical`: Base-only hook; upstream response bytes reach the client unchanged (raw `Serve` test).
- [ ] **Step 2: Run `go test -race ./...` — expect FAIL.**
- [ ] **Step 3: Implement** (add a `Resource` option to `testkit.Server` if needed: `testkit.ResourceSpec{URI, Text string}`).
- [ ] **Step 4: Run `go test -race ./...` — expect PASS.**
- [ ] **Step 5: Commit** `feat: response hooks, sessions and typed tool helpers`

---

### Task 5: Events and chain ID

**Files:**
- Create: `events.go`
- Modify: `proxy.go`, `testkit/testkit.go` (add `Recorder`)
- Test: `events_test.go`

**Interfaces:**
- Produces:
  - `type Event struct { V int \`json:"v"\`; Time time.Time \`json:"time"\`; Chain string \`json:"chain"\`; Session string \`json:"session"\`; Hop string \`json:"hop"\`; Kind string \`json:"kind"\`; ID string \`json:"id,omitempty"\`; Method string \`json:"method"\`; Tool string \`json:"tool,omitempty"\`; Params json.RawMessage \`json:"params,omitempty"\`; Decision string \`json:"decision,omitempty"\`; Hook string \`json:"hook,omitempty"\`; Result json.RawMessage \`json:"result,omitempty"\`; Error *jsonrpc.Error \`json:"error,omitempty"\`; LatencyMS float64 \`json:"latency_ms,omitempty"\` }`
  - `Kind` values: `request` (emitted once per hooked request after hooks decide; `Decision` = final decision string, `Hook` = the hook that made a terminal decision, `Params` = params as forwarded), `response` (emitted when the response is written to the client; `LatencyMS` from request receipt).
  - `const EventSchemaVersion = 1`; `type EventSink interface { Emit(Event) }`; `func NewJSONLSink(w io.Writer) EventSink` (one JSON object per line, mutex-guarded).
  - `const ChainEnv = "SWITCHYARD_CHAIN_ID"`; `func ChainIDFromEnv() string` (env value or new random 16-hex).
  - `func (d Decision) Annotate(label string) Decision` — attaches a free-form label consumers use to mark their decisions; event `Decision` becomes `<kind>:<label>` (e.g. `hang:fault-timeout`). AgentChaos relies on this.
  - `testkit.Recorder` implementing `EventSink` with `Events() []switchyard.Event`.

- [ ] **Step 1: Write failing tests:**
  - `TestEventsForToolCall`: one `echo` call → events `request` (decision `continue`, tool `echo`) then `response` (latency > 0), both with `v: 1`, same `id`, `chain` = Options.ChainID, `hop` = Options.Name.
  - `TestEventDecisionAnnotated`: hook returns `Hang().Annotate("fault-timeout")`; request event decision `hang:fault-timeout`, `hook` set.
  - `TestJSONLSink`: two events → two lines, each valid JSON with `"v":1`.
  - `TestChainIDFromEnv`: with env set returns it; unset returns 16 hex chars, different each call.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement.** If `Options.Sink` is nil, emit nothing.
- [ ] **Step 4: Run `go test -race ./...` — expect PASS.**
- [ ] **Step 5: Commit** `feat: versioned JSONL events and chain IDs`

---

### Task 6: RunCommand, debug binary, chain end-to-end

**Files:**
- Create: `command.go`, `internal/testserver/main.go`, `cmd/switchyard/main.go`
- Test: `e2e_test.go`, `command_test.go`

**Interfaces:**
- Produces:
  - `func RunCommand(ctx context.Context, p *Proxy, argv []string, down io.Reader, downOut io.Writer) error` — starts `argv` with `ChainEnv` set to the proxy's chain ID, upstream stderr inherited, then `Serve`s; on return, waits for the child (kills it if still running after 2 s of upstream stdin being closed).
  - `switchyard [--name NAME] [--trace FILE] -- <cmd> [args...]`: pass-through proxy; chain ID from `ChainIDFromEnv()`; trace JSONL to `FILE` (default: none). Never writes to stdout except protocol.
  - `internal/testserver`: stdio MCP server (SDK) with tools `echo` and `env` (returns `SWITCHYARD_CHAIN_ID` it sees).

- [ ] **Step 1: Write failing tests** (`go build` both binaries into `t.TempDir()` in `TestMain`):
  - `TestChainOfTwoProxies`: SDK client via `mcp.CommandTransport` runs `switchyard --name a --trace A -- switchyard --name b --trace B -- testserver`; `echo` works; `env` tool returns the same chain ID found in both traces; both traces contain the `echo` request.
  - `TestChainIDGeneratedOnce`: with `SWITCHYARD_CHAIN_ID` unset, both hops' traces share one generated ID.
  - `TestUpstreamCommandMissing`: `RunCommand` with a nonexistent binary returns an error naming it.
  - `BenchmarkHop`: echo round trip direct vs through one proxy (in-process via testkit); report ns/op. Record the result in the commit message; no threshold assertion in v0.
- [ ] **Step 2: Run `go test ./...` — expect FAIL.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run `go test -race ./... && go vet ./...` — expect PASS; run `go test -bench Hop -run ^$ .` and note per-hop overhead.**
- [ ] **Step 5: Commit** `feat: RunCommand, switchyard debug binary, chain e2e test`
