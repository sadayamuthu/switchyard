// Package jsonrpc implements newline-delimited JSON-RPC 2.0 framing that
// preserves each message's original bytes.
//
// The bytes within a line are kept exactly, but the framing is normalized: a
// Reader strips a line's "\n" or "\r\n" ending and skips blank lines, and a
// Writer ends every message with "\n". So CRLF line endings become LF, blank
// lines are dropped, and an unterminated final line gains a newline.
package jsonrpc

import (
	"bytes"
	"encoding/json"
)

// Kind classifies a JSON-RPC message.
type Kind int

const (
	// KindRequest has both id and method.
	KindRequest Kind = iota
	// KindNotification has method but no id.
	KindNotification
	// KindResponse has id and result or error, and no method.
	KindResponse
	// KindOther is a batch array, non-object, invalid JSON, or an object
	// matching none of the other kinds.
	KindOther
)

// Error is a JSON-RPC error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Message is a parsed JSON-RPC message. Raw holds the original line without
// the trailing newline.
type Message struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
	Result json.RawMessage
	Error  *Error
	Raw    []byte
}

type wire struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Parse never fails; unparseable input yields a KindOther message.
// Raw aliases the input slice; callers must not reuse the buffer.
func Parse(line []byte) *Message {
	m := &Message{Raw: line}
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return m
	}
	var w wire
	if err := json.Unmarshal(trimmed, &w); err != nil {
		return &Message{Raw: line}
	}
	m.ID, m.Method, m.Params, m.Result, m.Error = w.ID, w.Method, w.Params, w.Result, w.Error
	return m
}

// Kind classifies the message.
func (m *Message) Kind() Kind {
	hasID := len(m.ID) > 0
	switch {
	case hasID && m.Method != "":
		return KindRequest
	case m.Method != "":
		return KindNotification
	case hasID && (len(m.Result) > 0 || m.Error != nil):
		return KindResponse
	}
	return KindOther
}

// IDKey returns the raw ID bytes as a string, suitable as a map key.
func (m *Message) IDKey() string { return string(m.ID) }

// codeInternalError is the JSON-RPC internal error code.
const codeInternalError = -32603

// NewResponse builds a success response. Empty result is sent as null. If
// result is not valid JSON, it builds a -32603 "internal error" response
// instead, so Raw is never empty.
func NewResponse(id json.RawMessage, result json.RawMessage) *Message {
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	raw, err := Marshal(wire{JSONRPC: "2.0", ID: id, Result: result})
	if err != nil {
		return internalError(id)
	}
	return &Message{ID: id, Result: result, Raw: raw}
}

// NewErrorResponse builds an error response. If e is nil or its Data is not
// valid JSON, it builds a -32603 "internal error" response without data
// instead, so Raw is never empty and always carries an error.
func NewErrorResponse(id json.RawMessage, e *Error) *Message {
	if e == nil {
		return internalError(id)
	}
	raw, err := Marshal(wire{JSONRPC: "2.0", ID: id, Error: e})
	if err != nil {
		return internalError(id)
	}
	return &Message{ID: id, Error: e, Raw: raw}
}

// internalError builds a -32603 "internal error" response to id, or to a null
// id if id itself is not valid JSON.
func internalError(id json.RawMessage) *Message {
	e := &Error{Code: codeInternalError, Message: "internal error"}
	raw, err := Marshal(wire{JSONRPC: "2.0", ID: id, Error: e})
	if err != nil {
		id = json.RawMessage("null")
		raw, _ = Marshal(wire{JSONRPC: "2.0", ID: id, Error: e}) // always encodes
	}
	return &Message{ID: id, Error: e, Raw: raw}
}

// Marshal is json.Marshal without HTML escaping, so that <, > and & keep
// their bytes (a re-encoded id must stay byte-identical to the request's).
// json.RawMessage values are still compacted, and must be valid JSON or
// Marshal returns an error.
func Marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}
