package switchyard

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

// EventSchemaVersion is the value of Event.V. It changes only when the event
// schema changes incompatibly.
const EventSchemaVersion = 1

// Event kinds.
const (
	// KindRequest is emitted once per hooked request, after the hooks decide.
	KindRequest = "request"
	// KindResponse is emitted when a response is written to the client.
	KindResponse = "response"
)

// Event is one structured, versioned record of a hooked request or its
// response. Events are written as one JSON object per line by NewJSONLSink.
type Event struct {
	V       int       `json:"v"`
	Time    time.Time `json:"time"`
	Chain   string    `json:"chain"`
	Session string    `json:"session"`
	Hop     string    `json:"hop"`
	// Kind is KindRequest or KindResponse.
	Kind string `json:"kind"`
	// ID is the request's JSON-RPC id as it appears on the wire (a string id
	// keeps its quotes).
	ID     string `json:"id,omitempty"`
	Method string `json:"method"`
	// Tool is the tool name of a tools/call request.
	Tool string `json:"tool,omitempty"`
	// Params, on a request event, are the params as forwarded.
	Params json.RawMessage `json:"params,omitempty"`
	// Decision, on a request event, is the final decision's String, such as
	// "continue" or "hang:fault-timeout".
	Decision string `json:"decision,omitempty"`
	// Hook, on a request event, names the hook that made a terminal decision.
	Hook   string          `json:"hook,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *jsonrpc.Error  `json:"error,omitempty"`
	// LatencyMS, on a response event, is the time since the request arrived.
	LatencyMS float64 `json:"latency_ms,omitempty"`
}

// EventSink receives proxy events. Emit may be called concurrently, and is
// called on the proxy's goroutines, so it should return quickly. A panic in
// Emit is recovered and the event dropped.
type EventSink interface {
	Emit(Event)
}

type jsonlSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewJSONLSink returns a sink writing each event to w as one line of JSON.
// It is safe for concurrent use. An event that cannot be encoded or written
// is dropped.
func NewJSONLSink(w io.Writer) EventSink { return &jsonlSink{w: w} }

func (s *jsonlSink) Emit(e Event) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode ends the value with a newline
	enc.SetEscapeHTML(false)
	if enc.Encode(e) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.w.Write(buf.Bytes())
}

// ChainEnv is the environment variable carrying the chain ID between stacked
// proxies.
const ChainEnv = "SWITCHYARD_CHAIN_ID"

// ChainIDFromEnv returns the chain ID in the environment, or a new random
// 16-hex-character one if there is none. It does not set the environment.
func ChainIDFromEnv() string {
	if v := os.Getenv(ChainEnv); v != "" {
		return v
	}
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.
	return hex.EncodeToString(b[:])
}

// emit sends e to the sink, dropping it if the sink panics.
func (c *conn) emit(e Event) {
	defer func() { _ = recover() }()
	e.V = EventSchemaVersion
	e.Time = time.Now()
	e.Chain = c.session.chainID
	e.Session = c.session.id
	e.Hop = c.hop
	c.sink.Emit(e)
}

// emitRequest records the hooks' final decision d on e's request.
func (c *conn) emitRequest(e *pendingEntry, d Decision) {
	if c.sink == nil || e.req == nil {
		return
	}
	ev := Event{
		Kind:     KindRequest,
		ID:       string(e.id),
		Method:   e.req.Method,
		Params:   e.req.Params,
		Decision: d.String(),
		Hook:     d.by,
	}
	if tc, ok := e.req.ToolCall(); ok {
		ev.Tool = tc.Name
	}
	c.emit(ev)
}

// replyFor writes m, the answer to e's request, to the client, and records it
// as a response event if the write was accepted.
func (c *conn) replyFor(e *pendingEntry, m *jsonrpc.Message) {
	if c.send(c.toClient, m) != nil || c.sink == nil || e.req == nil {
		return
	}
	c.emit(Event{
		Kind:      KindResponse,
		ID:        string(e.id),
		Method:    e.req.Method,
		Result:    m.Result,
		Error:     m.Error,
		LatencyMS: float64(time.Since(e.start)) / float64(time.Millisecond),
	})
}
