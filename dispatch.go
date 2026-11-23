package switchyard

import (
	"context"
	"encoding/json"
	"fmt"
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
		ctx:    ctx,
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
		if !c.pending.markForwarded(key, e) {
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

// hookedResponse runs the OnResponse hooks on m, the upstream's response to
// the forwarded request e, and returns the message to send the client. The
// hooks run on another goroutine so that one ignoring its context cannot
// keep the upstream read loop, and so Serve, from returning: ok is false if
// Serve aborted first.
func (c *conn) hookedResponse(e *pendingEntry, m *jsonrpc.Message) (out *jsonrpc.Message, ok bool) {
	done := make(chan *jsonrpc.Message, 1)
	go func() { done <- c.respond(e, m) }()
	select {
	case out := <-done:
		return out, true
	case <-c.aborted:
		return nil, false
	}
}

// respond runs the OnResponse hooks in reverse order on m, the response to
// e, each receiving the previous one's output. If none changed it, m itself
// is returned, so its original bytes are forwarded. For tools/list, the
// final tools are recorded in the session.
func (c *conn) respond(e *pendingEntry, m *jsonrpc.Message) *jsonrpc.Message {
	orig := &Response{Result: m.Result, Error: m.Error}
	resp := orig
	for i := len(c.hooks) - 1; i >= 0; i-- {
		h := c.hooks[i]
		next, panicked := callResponseHook(e.ctx, h, c.session, e.req, resp)
		switch {
		case panicked:
			return hookError(h, "panicked").response(m.ID)
		case next == nil || next == resp:
			continue
		case !validResponse(next):
			return hookError(h, "returned an invalid result").response(m.ID)
		}
		resp = next
	}
	if e.req.Method == methodToolsList {
		if tools, ok := resp.ToolList(); ok {
			c.session.setTools(tools)
		}
	}
	switch {
	case resp == orig:
		return m
	case resp.Error != nil:
		return jsonrpc.NewErrorResponse(m.ID, resp.Error)
	default:
		return jsonrpc.NewResponse(m.ID, resp.Result)
	}
}

// validResponse reports whether r's JSON members are valid: Error.Data if r
// is an error, otherwise Result (which may be empty, meaning null).
func validResponse(r *Response) bool {
	if r.Error != nil {
		return len(r.Error.Data) == 0 || json.Valid(r.Error.Data)
	}
	return len(r.Result) == 0 || json.Valid(r.Result)
}

// callResponseHook calls h.OnResponse, reporting a panic.
func callResponseHook(ctx context.Context, h Hook, s *Session, r *Request, resp *Response) (out *Response, panicked bool) {
	defer func() {
		if recover() != nil {
			out, panicked = nil, true
		}
	}()
	return h.OnResponse(ctx, s, r, resp), false
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
// pending entry. Like Parse, it matches the member name case-insensitively.
func withParams(m *jsonrpc.Message, params json.RawMessage) *jsonrpc.Message {
	raw, ok := setMember(m.Raw, "params", func(k string) bool { return strings.EqualFold(k, "params") }, params)
	if !ok {
		return m // unreachable: m parsed as a request
	}
	return &jsonrpc.Message{ID: m.ID, Method: m.Method, Params: params, Raw: raw}
}
