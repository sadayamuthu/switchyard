// Package switchyard proxies MCP stdio traffic between a client and its
// upstream tool server. With no hooks it is transparent: every message is
// forwarded as its original bytes.
package switchyard

import (
	"context"
	"errors"
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

// Hook intercepts client→server requests. It is a placeholder; hooks are not
// yet invoked.
type Hook interface{}

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
func New(opts Options) *Proxy { return &Proxy{opts: opts} }

// Serve proxies one session until the client or the server finishes.
//
// down is client→proxy, downOut is proxy→client, up is server→proxy (the
// server's stdout) and upOut is proxy→server (the server's stdin).
//
// When down reaches EOF, Serve closes upOut (if it is an io.Closer), waits for
// up to reach EOF so in-flight responses still reach the client, and returns
// nil. When up ends first, every pending client request is answered with a
// -32603 "upstream closed" error, upOut and down are closed (if they are
// io.Closers) and Serve returns ErrUpstreamClosed. When ctx is done, all
// closable streams are closed and Serve returns ctx.Err().
//
// Serve returns only after both of its reading goroutines have exited, so a
// stream that is not an io.Closer must reach EOF on its own for Serve to
// return in the cases where Serve would otherwise close it.
func (p *Proxy) Serve(ctx context.Context, down io.Reader, downOut io.Writer, up io.Reader, upOut io.Writer) error {
	c := &conn{
		session:  newSession(p.opts.ChainID),
		toClient: jsonrpc.NewWriter(downOut),
		toServer: jsonrpc.NewWriter(upOut),
		pending:  newPendingTable(),
	}
	p.pending.Store(c.pending)

	downDone := make(chan error, 1)
	upDone := make(chan error, 1)
	go func() { downDone <- c.downLoop(jsonrpc.NewReader(down)) }()
	go func() { upDone <- c.upLoop(jsonrpc.NewReader(up)) }()

	var upOutOnce sync.Once
	closeUpOut := func() { upOutOnce.Do(func() { closeIfCloser(upOut) }) }

	select {
	case err := <-downDone:
		closeUpOut()
		if err != nil {
			closeIfCloser(up)
		}
		select {
		case <-upDone:
		case <-ctx.Done():
			closeIfCloser(up)
			<-upDone
		}
		return err
	case err := <-upDone:
		closeUpOut()
		closeIfCloser(down)
		<-downDone
		if err != nil {
			return err
		}
		return ErrUpstreamClosed
	case <-ctx.Done():
		closeUpOut()
		closeIfCloser(down)
		closeIfCloser(up)
		<-downDone
		<-upDone
		return ctx.Err()
	}
}

// pendingLen reports the number of requests awaiting an upstream response in
// the most recent Serve call.
func (p *Proxy) pendingLen() int {
	if t := p.pending.Load(); t != nil {
		return t.len()
	}
	return 0
}

// conn is the per-Serve state. toClient and toServer are the only writers to
// downOut and upOut respectively.
type conn struct {
	session  *Session
	toClient *jsonrpc.Writer
	toServer *jsonrpc.Writer
	pending  *pendingTable
}

// downLoop forwards client→server messages, recording client requests as
// pending. It returns nil at EOF. A failed write upstream answers the request
// with an "upstream closed" error and keeps reading; the upstream reader will
// see EOF and end the session.
func (c *conn) downLoop(r *jsonrpc.Reader) error {
	for {
		m, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		isRequest := m.Kind() == jsonrpc.KindRequest
		if isRequest && !c.pending.add(m.IDKey(), &pendingEntry{id: m.ID}) {
			if err := c.toClient.Write(upstreamClosed(m.ID)); err != nil {
				return err
			}
			continue
		}
		if err := c.toServer.Write(m); err != nil && isRequest {
			if e, ok := c.pending.remove(m.IDKey()); ok {
				if err := c.toClient.Write(upstreamClosed(e.id)); err != nil {
					return err
				}
			}
		}
	}
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
			c.pending.remove(m.IDKey())
		}
		if err := c.toClient.Write(m); err != nil {
			return err
		}
	}
}

func (c *conn) failPending() {
	for _, e := range c.pending.close() {
		_ = c.toClient.Write(upstreamClosed(e.id))
	}
}

func upstreamClosed(id []byte) *jsonrpc.Message {
	return jsonrpc.NewErrorResponse(id, &jsonrpc.Error{Code: codeInternalError, Message: "upstream closed"})
}

func closeIfCloser(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}
