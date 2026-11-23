// Package jsonrpc implements newline-delimited JSON-RPC 2.0 framing that
// preserves each message's original bytes.
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

// NewResponse builds a success response.
func NewResponse(id json.RawMessage, result json.RawMessage) *Message {
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	raw, _ := json.Marshal(wire{JSONRPC: "2.0", ID: id, Result: result})
	return &Message{ID: id, Result: result, Raw: raw}
}

// NewErrorResponse builds an error response.
func NewErrorResponse(id json.RawMessage, e *Error) *Message {
	raw, _ := json.Marshal(wire{JSONRPC: "2.0", ID: id, Error: e})
	return &Message{ID: id, Error: e, Raw: raw}
}
