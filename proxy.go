// Package switchyard proxies MCP stdio traffic between a client and its
// upstream tool server. With no hooks it is transparent: every message is
// forwarded as its original bytes.
package switchyard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

// ErrUpstreamClosed is returned by Serve when the upstream server's output
// ends before the client's input does.
var ErrUpstreamClosed = errors.New("switchyard: upstream closed")

// codeInternalError is the JSON-RPC internal error code.
const codeInternalError = -32603

// EventSink receives proxy events. It is a placeholder; no events are emitted
// yet.
type EventSink interface{}

// Options configures a Proxy.
type Options struct {
	// Name identifies this hop.
	Name string
	// Hooks run, in order, on client→server requests.
	Hooks []Hook
	// Sink receives events; nil disables them.
	Sink EventSink
	// ChainID is shared by every hop proxying the same session.
	ChainID string
}

// Proxy forwards newline-delimited JSON-RPC between a client and a server.
type Proxy struct {
	opts    Options
	pending atomic.Pointer[pendingTable] // table of the most recent Serve
}

// New returns a Proxy configured by opts.
func New(opts Options) *Proxy {
	opts.Hooks = append([]Hook(nil), opts.Hooks...)
	return &Proxy{opts: opts}
}

// Serve proxies one session until the client or the server finishes.
//
// down is client→proxy, downOut is proxy→client, up is server→proxy (the
// server's stdout) and upOut is proxy→server (the server's stdin).
//
// The session ends when down reaches EOF, when up ends, or when ctx is done;
// at that point every hook context is cancelled.
//
// When down reaches EOF, Serve waits for in-flight tools/call hooks to decide,
// so their requests are still forwarded or answered, then closes upOut (if it
// is an io.Closer), waits for up to reach EOF so in-flight responses still
// reach the client, and returns nil. When up ends first, every pending client
// request is answered with a -32603 "upstream closed" error, upOut and down
// are closed (if they are io.Closers) and Serve returns ErrUpstreamClosed.
// When ctx is done, all four streams are closed (if they are io.Closers) and
// Serve returns ctx.Err(); ctx also cuts short any of the waits above.
//
// A failed write to downOut is not an error: the message is dropped, since a
// client that has gone away is reported by down reaching EOF.
//
// Serve returns only after both of its reading goroutines have exited, so a
// stream that is not an io.Closer must reach EOF on its own for Serve to
// return in the cases where Serve would otherwise close it. Serve does not
// wait for a hook that is still running once the session has ended (beyond
// the EOF wait above); whatever such a hook decides is discarded.
func (p *Proxy) Serve(ctx context.Context, down io.Reader, downOut io.Writer, up io.Reader, upOut io.Writer) error {
	sessCtx, endSession := context.WithCancel(ctx)
	defer endSession()
	c := &conn{
		ctx:      sessCtx,
		session:  newSession(p.opts.ChainID),
		hooks:    p.opts.Hooks,
		toClient: jsonrpc.NewWriter(downOut),
		toServer: jsonrpc.NewWriter(upOut),
		pending:  newPendingTable(),
	}
	p.pending.Store(c.pending)

	var downErr, upErr error
	downDone := make(chan struct{})
	upDone := make(chan struct{})
	go func() { downErr = c.downLoop(jsonrpc.NewReader(down)); close(downDone) }()
	go func() { upErr = c.upLoop(jsonrpc.NewReader(up)); close(upDone) }()

	closeDown, closeDownOut := closeOnce(down), closeOnce(downOut)
	closeUp, closeUpOut := closeOnce(up), closeOnce(upOut)
	ctxDone := ctx.Done()
	abort := func() {
		closeUpOut()
		closeDown()
		closeUp()
		closeDownOut()
		ctxDone = nil
	}
	// await waits for ch; if ctx is done first, it closes every stream, which
	// unblocks any loop or write still in progress, and keeps waiting.
	await := func(ch <-chan struct{}) {
		for {
			select {
			case <-ch:
				return
			case <-ctxDone:
				abort()
			}
		}
	}

	var err error
	select {
	case <-downDone:
		err = downErr
		endSession()
		if err == nil {
			select {
			case <-c.hooksIdle():
			case <-ctxDone:
				abort()
			}
		} else {
			closeUp()
		}
		closeUpOut()
	case <-upDone:
		err = upErr
		if err == nil {
			err = ErrUpstreamClosed
		}
		endSession()
		closeUpOut()
		closeDown()
	case <-ctxDone:
		err = ctx.Err()
		endSession()
		abort()
	}
	await(downDone)
	await(upDone)
	await(c.closeGate())
	return err
}

// pendingLen reports the number of requests awaiting a response in the most
// recent Serve call.
func (p *Proxy) pendingLen() int {
	if t := p.pending.Load(); t != nil {
		return t.len()
	}
	return 0
}

// errSessionEnded is returned by conn.send once Serve is returning.
var errSessionEnded = errors.New("switchyard: session ended")

// methodCancelled is the MCP notification a client sends to cancel a request.
const methodCancelled = "notifications/cancelled"

// conn is the per-Serve state. toClient and toServer are the only writers to
// downOut and upOut respectively, and are written only through send.
type conn struct {
	ctx      context.Context // cancelled when the session ends
	session  *Session
	hooks    []Hook
	toClient *jsonrpc.Writer
	toServer *jsonrpc.Writer
	pending  *pendingTable

	inflight sync.WaitGroup // tools/call requests being hooked

	gate  sync.RWMutex // held for reading during every write
	ended bool         // under gate: Serve is returning, write nothing
}

// send writes m unless Serve is returning, so a hook goroutine that outlives
// Serve never touches the streams.
func (c *conn) send(w *jsonrpc.Writer, m *jsonrpc.Message) error {
	c.gate.RLock()
	defer c.gate.RUnlock()
	if c.ended {
		return errSessionEnded
	}
	return w.Write(m)
}

// reply writes m to the client. A failed write is dropped: see Serve.
func (c *conn) reply(m *jsonrpc.Message) { _ = c.send(c.toClient, m) }

// closeGate stops all further writes, once writes in progress finish. The
// returned channel is closed when it has.
func (c *conn) closeGate() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		c.gate.Lock()
		c.ended = true
		c.gate.Unlock()
		close(done)
	}()
	return done
}

// hooksIdle returns a channel closed once no tools/call request is being
// hooked. Call it only after downLoop has returned.
func (c *conn) hooksIdle() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		c.inflight.Wait()
		close(done)
	}()
	return done
}

// downLoop forwards client→server messages, running hooks on client requests
// and recording them as pending. It returns nil at EOF. A failed write
// upstream answers the request with an "upstream closed" error and keeps
// reading; the upstream reader will see EOF and end the session.
func (c *conn) downLoop(r *jsonrpc.Reader) error {
	for {
		m, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch m.Kind() {
		case jsonrpc.KindRequest:
			if len(c.hooks) > 0 {
				if !c.hookRequest(m) {
					return c.ctx.Err()
				}
				continue
			}
			e := &pendingEntry{id: m.ID}
			if !c.pending.add(m.IDKey(), e) {
				c.reply(upstreamClosed(m.ID))
				continue
			}
			c.forward(m, m.IDKey(), e)
		case jsonrpc.KindNotification:
			if m.Method == methodCancelled {
				c.clientCancelled(m.Params)
			}
			_ = c.send(c.toServer, m)
		default:
			_ = c.send(c.toServer, m)
		}
	}
}

// forward sends a pending request upstream. If that fails and the request is
// still e, it is answered with an "upstream closed" error.
func (c *conn) forward(m *jsonrpc.Message, key string, e *pendingEntry) {
	if err := c.send(c.toServer, m); err != nil && c.pending.removeIf(key, e) {
		e.release()
		c.reply(upstreamClosed(e.id))
	}
}

// clientCancelled handles notifications/cancelled: the request is no longer
// pending, and its hook context is cancelled. The caller still forwards the
// notification upstream.
func (c *conn) clientCancelled(params json.RawMessage) {
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if err := json.Unmarshal(params, &p); err != nil || len(p.RequestID) == 0 {
		return
	}
	if e, ok := c.pending.remove(string(p.RequestID)); ok {
		e.release()
	}
}

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
// params is empty). Other members are kept; member order is not.
func withParams(m *jsonrpc.Message, params json.RawMessage) *jsonrpc.Message {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(m.Raw, &obj); err != nil {
		return m // unreachable: m parsed as a request
	}
	if len(params) == 0 {
		delete(obj, "params")
	} else {
		obj["params"] = params
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return m // unreachable: every member is valid JSON
	}
	return &jsonrpc.Message{ID: m.ID, Method: m.Method, Params: params, Raw: raw}
}

// upLoop forwards server→client messages, clearing pending entries for
// responses. When it ends, for any reason, every request still pending is
// answered with an "upstream closed" error.
func (c *conn) upLoop(r *jsonrpc.Reader) error {
	defer c.failPending()
	for {
		m, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if m.Kind() == jsonrpc.KindResponse {
			if e, ok := c.pending.remove(m.IDKey()); ok {
				e.release()
			}
		}
		c.reply(m)
	}
}

// failPending answers every pending request with an "upstream closed" error,
// except those a hook chose to Hang, and stops new ones being recorded.
func (c *conn) failPending() {
	for _, e := range c.pending.close() {
		e.release()
		if !e.hung {
			c.reply(upstreamClosed(e.id))
		}
	}
}

func upstreamClosed(id []byte) *jsonrpc.Message {
	return jsonrpc.NewErrorResponse(id, &jsonrpc.Error{Code: codeInternalError, Message: "upstream closed"})
}

// closeOnce returns a function closing v, if it is an io.Closer, on its first
// call only.
func closeOnce(v any) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			if c, ok := v.(io.Closer); ok {
				_ = c.Close()
			}
		})
	}
}
