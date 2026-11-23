package switchyard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

// hookRequest records m as pending and runs the hooks on it: on a new
// goroutine for tools/call, otherwise before returning. It returns false if
// the session ended while a synchronous hook was still running.
func (c *conn) hookRequest(m *jsonrpc.Message) bool {
	key := m.IDKey()
	ctx, cancel := context.WithCancel(c.ctx)
	e := &pendingEntry{
		id:     m.ID,
		req:    &Request{ID: m.ID, Method: m.Method, Params: m.Params},
		cancel: cancel,
	}
	if !c.pending.add(key, e) {
		cancel()
		c.reply(upstreamClosed(m.ID))
		return true
	}
	if m.Method == methodToolsCall {
		c.inflight.Add(1)
		go func() {
			defer c.inflight.Done()
			c.dispatch(m, key, e, c.evaluate(ctx, e.req))
		}()
		return true
	}
	// Evaluate on another goroutine so a hook that ignores its context
	// cannot keep the read loop, and so Serve, from returning.
	decided := make(chan Decision, 1)
	go func() { decided <- c.evaluate(ctx, e.req) }()
	select {
	case d := <-decided:
		c.dispatch(m, key, e, d)
		return true
	case <-c.ctx.Done():
		return false
	}
}

// evaluate runs the hooks on r in order until one makes a terminal decision,
// which it returns. Otherwise it returns ContinueWith carrying the final
// params if any hook rewrote them, or Continue.
func (c *conn) evaluate(ctx context.Context, r *Request) Decision {
	rewritten := false
	for _, h := range c.hooks {
		d := callHook(ctx, h, c.session, r)
		switch {
		case d.kind == kindContinueWith && len(d.params) > 0 && !json.Valid(d.params):
			return hookError(h, "returned invalid params")
		case d.kind == kindRespond && len(d.result) > 0 && !json.Valid(d.result):
			return hookError(h, "returned an invalid result")
		case d.kind == kindContinueWith:
			r.Params = d.params
			rewritten = true
		case d.terminal():
			return d
		}
	}
	if rewritten {
		return ContinueWith(r.Params)
	}
	return Continue()
}

// dispatch carries out decision d for request m, whose pending entry is e.
// A request no longer pending (cancelled by the client, or answered because
// upstream closed) is dropped.
func (c *conn) dispatch(m *jsonrpc.Message, key string, e *pendingEntry, d Decision) {
	switch d.kind {
	case kindContinue, kindContinueWith:
		if !c.pending.has(key, e) {
			return
		}
		if d.kind == kindContinueWith {
			m = withParams(m, d.params)
		}
		c.forward(m, key, e)
	case kindHang:
		c.pending.markHung(key, e)
	default:
		if c.pending.removeIf(key, e) {
			e.release()
			c.reply(d.response(m.ID))
		}
	}
}

// callHook calls h.OnRequest, turning a panic into a Block.
func callHook(ctx context.Context, h Hook, s *Session, r *Request) (d Decision) {
	defer func() {
		if recover() != nil {
			d = hookError(h, "panicked")
		}
	}()
	return h.OnRequest(ctx, s, r)
}

// hookError blocks a request with a -32603 "hook <name> <what>" error.
func hookError(h Hook, what string) Decision {
	return Block(&jsonrpc.Error{Code: codeInternalError, Message: "hook " + hookName(h) + " " + what})
}

// hookName returns h.Name(), or h's type if Name panics.
func hookName(h Hook) (name string) {
	defer func() {
		if recover() != nil {
			name = fmt.Sprintf("%T", h)
		}
	}()
	return h.Name()
}

// withParams returns m with its params member replaced by params (removed if
// params is empty, appended if absent). The new params is spliced into m.Raw,
// so every other byte, including the id, is kept exactly: re-encoding would
// compact and HTML-escape members, and a changed id no longer matches its
// pending entry. Like Parse, it matches the member name case-insensitively,
// and replaces every occurrence.
func withParams(m *jsonrpc.Message, params json.RawMessage) *jsonrpc.Message {
	spans, members, closing, ok := paramsSpans(m.Raw)
	if !ok {
		return m // unreachable: m parsed as a request
	}
	raw := append([]byte(nil), m.Raw...)
	switch {
	case len(spans) == 0 && len(params) > 0:
		member := append([]byte(`"params":`), params...)
		if members > 0 {
			member = append([]byte(","), member...)
		}
		raw = slices.Insert(raw, closing, member...)
	case len(params) > 0:
		for i := len(spans) - 1; i >= 0; i-- {
			raw = slices.Replace(raw, spans[i].valueStart, spans[i].valueEnd, params...)
		}
	default:
		for i := len(spans) - 1; i >= 0; i-- {
			start, end := spans[i].memberStart, spans[i].valueEnd
			if spans[i].first {
				// No comma precedes the first member; drop the one after it.
				j := end
				for j < len(raw) && isSpace(raw[j]) {
					j++
				}
				if j < len(raw) && raw[j] == ',' {
					end = j + 1
				}
			}
			raw = slices.Delete(raw, start, end)
		}
	}
	return &jsonrpc.Message{ID: m.ID, Method: m.Method, Params: params, Raw: raw}
}

// paramsSpan locates one params member in a raw JSON object. memberStart is
// just after the preceding value (or the opening brace), so the span from
// memberStart to valueEnd includes the separating comma unless first.
type paramsSpan struct {
	memberStart, valueStart, valueEnd int
	first                             bool
}

// paramsSpans returns the spans of raw's params members, raw's member count
// and the offset of raw's closing brace. ok is false if raw is not a JSON
// object.
func paramsSpans(raw []byte) (spans []paramsSpan, members, closing int, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, 0, 0, false
	}
	for ; dec.More(); members++ {
		start := int(dec.InputOffset())
		t, err := dec.Token()
		if err != nil {
			return nil, 0, 0, false
		}
		key, _ := t.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, 0, 0, false
		}
		end := int(dec.InputOffset())
		if strings.EqualFold(key, "params") {
			spans = append(spans, paramsSpan{memberStart: start, valueStart: end - len(v), valueEnd: end, first: members == 0})
		}
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return nil, 0, 0, false
	}
	return spans, members, int(dec.InputOffset()) - 1, true
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
