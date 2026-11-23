package switchyard

import (
	"context"
	"encoding/json"

	"github.com/sadayamuthu/switchyard/jsonrpc"
)

// Request is a client→server JSON-RPC request as seen by hooks.
type Request struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
}

// Response is the response to a Request as seen by hooks: Result on success,
// Error on failure.
type Response struct {
	Result json.RawMessage
	Error  *jsonrpc.Error
}

// Hook intercepts client→server requests and their responses. Embed Base to
// get no-op defaults and implement only what is needed.
//
// OnRequest runs for every client→server request, in Options.Hooks order,
// until a hook returns a terminal Decision (anything but Continue or
// ContinueWith). tools/call requests are hooked on their own goroutine, so
// OnRequest may be called concurrently; all other requests are hooked one at
// a time in arrival order, and a slow OnRequest for them delays the client's
// later messages. ctx is cancelled when the client cancels the request or the
// session ends.
//
// A panic in OnRequest blocks the request with a -32603 "hook <name>
// panicked" error.
type Hook interface {
	// Name identifies the hook in errors and events.
	Name() string
	// OnRequest decides what happens to r. A hook must not modify r; it
	// returns ContinueWith to replace the params.
	OnRequest(ctx context.Context, s *Session, r *Request) Decision
	// OnResponse may observe or replace resp, the response to a forwarded
	// r. It returns resp unchanged to leave it alone.
	OnResponse(ctx context.Context, s *Session, r *Request, resp *Response) *Response
}

// Base is an embeddable Hook with no-op OnRequest and OnResponse. It does not
// implement Name.
type Base struct{}

// OnRequest returns Continue().
func (Base) OnRequest(context.Context, *Session, *Request) Decision { return Continue() }

// OnResponse returns resp.
func (Base) OnResponse(_ context.Context, _ *Session, _ *Request, resp *Response) *Response {
	return resp
}

type decisionKind uint8

const (
	kindContinue decisionKind = iota // zero value: a zero Decision continues
	kindContinueWith
	kindBlock
	kindBlockTool
	kindRespond
	kindHang
)

var kindNames = [...]string{
	kindContinue:     "continue",
	kindContinueWith: "continue-with",
	kindBlock:        "block",
	kindBlockTool:    "block-tool",
	kindRespond:      "respond",
	kindHang:         "hang",
}

// Decision is a hook's verdict on a request, made with Continue,
// ContinueWith, Block, BlockTool, Respond or Hang. The zero Decision is
// Continue().
type Decision struct {
	kind   decisionKind
	params json.RawMessage // ContinueWith
	err    *jsonrpc.Error  // Block
	result json.RawMessage // BlockTool, Respond
}

// Continue passes the request on to the next hook, or upstream after the last.
func Continue() Decision { return Decision{} }

// ContinueWith replaces the request's params and passes it on to the next
// hook, or upstream after the last. Empty params removes the params member.
//
// params replaces the old params wholesale, including params._meta (which
// carries, for example, the client's progressToken): a hook that needs _meta
// kept must copy it into params. Every other member of the request is
// forwarded byte-for-byte.
func ContinueWith(params json.RawMessage) Decision {
	return Decision{kind: kindContinueWith, params: params}
}

// Block answers the request with the JSON-RPC error e without sending it
// upstream. A nil e is replaced by a -32603 "blocked by hook" error.
func Block(e *jsonrpc.Error) Decision {
	if e == nil {
		e = &jsonrpc.Error{Code: codeInternalError, Message: "blocked by hook"}
	}
	return Decision{kind: kindBlock, err: e}
}

// BlockTool answers the request, without sending it upstream, with a
// successful tools/call result that reports a tool error:
// {"content":[{"type":"text","text":message}],"isError":true}. Clients
// show the message to the model, as they would a failing tool's output.
func BlockTool(message string) Decision {
	return Decision{kind: kindBlockTool, result: toolErrorResult(message)}
}

// Respond answers the request with result without sending it upstream.
// Empty result is sent as null.
func Respond(result json.RawMessage) Decision {
	return Decision{kind: kindRespond, result: result}
}

// Hang never answers the request and never sends it upstream. The request
// stays pending until the client cancels it or the session ends.
func Hang() Decision { return Decision{kind: kindHang} }

// String returns the decision's kind: continue, continue-with, block,
// block-tool, respond or hang.
func (d Decision) String() string {
	if int(d.kind) < len(kindNames) {
		return kindNames[d.kind]
	}
	return "unknown"
}

// terminal reports whether d stops hook evaluation.
func (d Decision) terminal() bool { return d.kind != kindContinue && d.kind != kindContinueWith }

// response returns the message answering request id for a Block, BlockTool
// or Respond decision.
func (d Decision) response(id json.RawMessage) *jsonrpc.Message {
	if d.kind == kindBlock {
		return jsonrpc.NewErrorResponse(id, d.err)
	}
	return jsonrpc.NewResponse(id, d.result)
}
