package switchyard_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sadayamuthu/switchyard"
	"github.com/sadayamuthu/switchyard/testkit"
)

func echoTool() testkit.ToolSpec {
	return testkit.ToolSpec{
		Name:        "echo",
		Description: "echoes text",
		Handler: func(_ context.Context, args map[string]any) (*mcp.CallToolResult, error) {
			text, _ := args["text"].(string)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
		},
	}
}

func resultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("content len = %d, want 1", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want *mcp.TextContent", res.Content[0])
	}
	return tc.Text
}

func TestPassThroughInitialize(t *testing.T) {
	viaProxy := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Name: "test"})
	direct := testkit.ConnectDirect(t, testkit.Server(t, echoTool()))

	got, want := viaProxy.InitializeResult(), direct.InitializeResult()
	if !reflect.DeepEqual(got.ServerInfo, want.ServerInfo) {
		t.Errorf("server info via proxy = %+v, direct = %+v", got.ServerInfo, want.ServerInfo)
	}
	if !reflect.DeepEqual(got.Capabilities, want.Capabilities) {
		t.Errorf("capabilities via proxy = %+v, direct = %+v", got.Capabilities, want.Capabilities)
	}
	if got.ProtocolVersion != want.ProtocolVersion {
		t.Errorf("protocol via proxy = %q, direct = %q", got.ProtocolVersion, want.ProtocolVersion)
	}
}

func TestPassThroughToolsListAndCall(t *testing.T) {
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{})
	ctx := t.Context()

	list, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(list.Tools) != 1 || list.Tools[0].Name != "echo" {
		t.Fatalf("tools = %+v, want [echo]", list.Tools)
	}

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hi"}})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if got := resultText(t, res); got != "hi" {
		t.Errorf("echo returned %q, want %q", got, "hi")
	}
}

func TestPassThroughServerToClient(t *testing.T) {
	server := testkit.Server(t)
	server.AddTool(&mcp.Tool{Name: "chatty", InputSchema: map[string]any{"type": "object"}},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if err := req.Session.Log(ctx, &mcp.LoggingMessageParams{Level: "info", Data: "hello from server"}); err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "done"}}}, nil
		})

	logs := make(chan any, 1)
	cs := testkit.ConnectWith(t, server, switchyard.Options{}, &mcp.ClientOptions{
		LoggingMessageHandler: func(_ context.Context, req *mcp.LoggingMessageRequest) {
			select {
			case logs <- req.Params.Data:
			default:
			}
		},
	})
	// The negotiated protocol carries the log level in each request's _meta.
	params := &mcp.CallToolParams{Name: "chatty", Meta: mcp.Meta{mcp.MetaKeyLogLevel: "debug"}}
	if _, err := cs.CallTool(t.Context(), params); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	select {
	case data := <-logs:
		if data != "hello from server" {
			t.Errorf("log data = %v, want %q", data, "hello from server")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client never received the logging notification")
	}
}

func TestPassThroughByteExact(t *testing.T) {
	downLines := []string{
		`{"params":{"b":2,"a":1},  "method":"tools/call","id":7,"jsonrpc":"2.0"}`,
		`[{"jsonrpc":"2.0","id":1,"method":"x"},{"jsonrpc":"2.0","method":"y"}]`,
		`this is not json`,
		`{"method":"notifications/initialized" ,"jsonrpc":"2.0"}`,
	}
	upLines := []string{
		`{"result":{"z":1, "a":[]},"id":7,"jsonrpc":"2.0"}`,
		`{"jsonrpc":"2.0","id":"unknown","result":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}`,
		`garbage from server`,
	}

	// Fake upstream: reads everything the proxy sends, then replies with
	// upLines and exits (closes its stdout), like a server on stdin EOF.
	upOutR, upOutW := io.Pipe()
	upR, upW := io.Pipe()
	var gotUp bytes.Buffer
	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		_, _ = io.Copy(&gotUp, upOutR)
		_, _ = io.WriteString(upW, strings.Join(upLines, "\n")+"\n")
		_ = upW.Close()
	}()

	p := switchyard.New(switchyard.Options{})
	var downOut bytes.Buffer
	down := strings.NewReader(strings.Join(downLines, "\n") + "\n")
	if err := p.Serve(t.Context(), down, &downOut, upR, upOutW); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	<-upstreamDone

	if want := strings.Join(downLines, "\n") + "\n"; gotUp.String() != want {
		t.Errorf("upstream got:\n%s\nwant:\n%s", gotUp.String(), want)
	}
	if want := strings.Join(upLines, "\n") + "\n"; downOut.String() != want {
		t.Errorf("client got:\n%s\nwant:\n%s", downOut.String(), want)
	}
	if n := switchyard.PendingLen(p); n != 0 {
		t.Errorf("pending = %d after Serve, want 0", n)
	}
}

func TestUpstreamClosesMidCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	server := testkit.Server(t, testkit.ToolSpec{
		Name: "block",
		Handler: func(ctx context.Context, _ map[string]any) (*mcp.CallToolResult, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &mcp.CallToolResult{}, nil
		},
	})

	// Wire client ↔ proxy ↔ server by hand so the test owns the upstream pipes.
	c2pR, c2pW := io.Pipe()
	p2cR, p2cW := io.Pipe()
	p2sR, p2sW := io.Pipe()
	s2pR, s2pW := io.Pipe()
	if _, err := server.Connect(t.Context(), &mcp.IOTransport{Reader: p2sR, Writer: s2pW}, nil); err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	p := switchyard.New(switchyard.Options{})
	served := make(chan error, 1)
	go func() { served <- p.Serve(t.Context(), c2pR, p2cW, s2pR, p2sW) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "v0"}, nil)
	cs, err := client.Connect(t.Context(), &mcp.IOTransport{Reader: p2cR, Writer: c2pW}, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	defer cs.Close()

	callErr := make(chan error, 1)
	go func() {
		_, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "block"})
		callErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("tool handler never entered")
	}

	// The upstream goes away mid-call: its stdin and stdout pipes close.
	_ = p2sR.Close()
	_ = s2pW.Close()

	select {
	case err := <-callErr:
		if err == nil || !strings.Contains(err.Error(), "upstream closed") {
			t.Fatalf("CallTool error = %v, want one containing %q", err, "upstream closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CallTool did not return after upstream closed")
	}
	select {
	case err := <-served:
		if !errors.Is(err, switchyard.ErrUpstreamClosed) {
			t.Errorf("Serve = %v, want ErrUpstreamClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after upstream closed")
	}
	if n := switchyard.PendingLen(p); n != 0 {
		t.Errorf("pending = %d after upstream closed, want 0", n)
	}
}

func TestServeReturnsErrUpstreamClosed(t *testing.T) {
	downR, downW := io.Pipe()
	defer downW.Close()
	p := switchyard.New(switchyard.Options{})
	err := p.Serve(t.Context(), downR, io.Discard, strings.NewReader(""), nopWriteCloser{io.Discard})
	if !errors.Is(err, switchyard.ErrUpstreamClosed) {
		t.Fatalf("Serve = %v, want ErrUpstreamClosed", err)
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// TestPassThroughServerToClientRequest checks that a server→client request
// (sampling/createMessage) and the client's response to it round-trip
// through the proxy, with and without a hook installed.
func TestPassThroughServerToClientRequest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hooks []switchyard.Hook
	}{
		{"no hooks", nil},
		{"hook", []switchyard.Hook{&funcHook{name: "noop", fn: switchyard.Base{}.OnRequest}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := testkit.Server(t)
			server.AddTool(&mcp.Tool{Name: "sample", InputSchema: map[string]any{"type": "object"}},
				func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					res, err := req.Session.CreateMessage(ctx, &mcp.CreateMessageParams{
						MaxTokens: 10,
						Messages:  []*mcp.SamplingMessage{{Role: "user", Content: &mcp.TextContent{Text: "q"}}},
					})
					if err != nil {
						return nil, err
					}
					text, _ := res.Content.(*mcp.TextContent)
					if text == nil {
						return nil, fmt.Errorf("sampling content %T, want text", res.Content)
					}
					return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: res.Model + ": " + text.Text}}}, nil
				})
			clientOpts := &mcp.ClientOptions{
				CreateMessageHandler: func(_ context.Context, req *mcp.CreateMessageRequest) (*mcp.CreateMessageResult, error) {
					in, _ := req.Params.Messages[0].Content.(*mcp.TextContent)
					if in == nil {
						return nil, fmt.Errorf("sampling message content %T, want text", req.Params.Messages[0].Content)
					}
					return &mcp.CreateMessageResult{Model: "m", Role: "assistant", Content: &mcp.TextContent{Text: "sampled " + in.Text}}, nil
				},
			}
			// Server-initiated sampling needs a protocol before 2026-07-28.
			cs, _ := connectWithProxyClient(t, server, switchyard.Options{Hooks: tc.hooks}, clientOpts,
				&mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "sample"})
			if err != nil {
				t.Fatalf("CallTool: %v", err)
			}
			if res.IsError {
				t.Fatalf("tool error: %+v", res.Content)
			}
			if got := resultText(t, res); got != "m: sampled q" {
				t.Errorf("result = %q, want %q", got, "m: sampled q")
			}
		})
	}
}
