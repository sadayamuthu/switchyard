package switchyard_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sadayamuthu/switchyard"
	"github.com/sadayamuthu/switchyard/jsonrpc"
	"github.com/sadayamuthu/switchyard/testkit"
)

// funcHook is a Hook whose OnRequest is a function.
type funcHook struct {
	switchyard.Base
	name string
	fn   func(ctx context.Context, s *switchyard.Session, r *switchyard.Request) switchyard.Decision
}

func (h *funcHook) Name() string { return h.name }

func (h *funcHook) OnRequest(ctx context.Context, s *switchyard.Session, r *switchyard.Request) switchyard.Decision {
	return h.fn(ctx, s, r)
}

// onTool returns a hook applying fn to tools/call requests for tool and
// continuing everything else.
func onTool(name, tool string, fn func(ctx context.Context, r *switchyard.Request) switchyard.Decision) *funcHook {
	return &funcHook{name: name, fn: func(ctx context.Context, _ *switchyard.Session, r *switchyard.Request) switchyard.Decision {
		if tc, ok := r.ToolCall(); ok && tc.Name == tool {
			return fn(ctx, r)
		}
		return switchyard.Continue()
	}}
}

// countedEcho is echoTool with a call counter.
func countedEcho(calls *atomic.Int32) testkit.ToolSpec {
	spec := echoTool()
	inner := spec.Handler
	spec.Handler = func(ctx context.Context, args map[string]any) (*mcp.CallToolResult, error) {
		calls.Add(1)
		return inner(ctx, args)
	}
	return spec
}

func slowTool() testkit.ToolSpec {
	return testkit.ToolSpec{
		Name: "slow",
		Handler: func(context.Context, map[string]any) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "slow"}}}, nil
		},
	}
}

func callEcho(t *testing.T, cs *mcp.ClientSession, text string) (*mcp.CallToolResult, error) {
	t.Helper()
	return cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": text}})
}

func TestContinueWithRewritesArgs(t *testing.T) {
	hook := onTool("rewrite", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.ContinueWith(json.RawMessage(`{"name":"echo","arguments":{"text":"bye"}}`))
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool(), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	res, err := callEcho(t, cs, "hi")
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if got := resultText(t, res); got != "bye" {
		t.Errorf("result text = %q, want %q", got, "bye")
	}
}

func TestBlockJSONRPC(t *testing.T) {
	var calls atomic.Int32
	hook := onTool("deny", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.Block(&jsonrpc.Error{Code: -32001, Message: "denied"})
	})
	cs := testkit.Connect(t, testkit.Server(t, countedEcho(&calls), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	_, err := callEcho(t, cs, "hi")
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("CallTool error = %v, want one containing %q", err, "denied")
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("upstream echo called %d times, want 0", n)
	}
}

func TestBlockTool(t *testing.T) {
	var calls atomic.Int32
	const msg = "echo is not allowed here"
	hook := onTool("deny", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.BlockTool(msg)
	})
	cs := testkit.Connect(t, testkit.Server(t, countedEcho(&calls), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	res, err := callEcho(t, cs, "hi")
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Errorf("IsError = false, want true")
	}
	if got := resultText(t, res); got != msg {
		t.Errorf("result text = %q, want %q", got, msg)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("upstream echo called %d times, want 0", n)
	}
}

func TestRespondSynthesized(t *testing.T) {
	var calls atomic.Int32
	hook := onTool("fake", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.Respond(json.RawMessage(`{"content":[{"type":"text","text":"fake"}]}`))
	})
	cs := testkit.Connect(t, testkit.Server(t, countedEcho(&calls), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	res, err := callEcho(t, cs, "hi")
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if got := resultText(t, res); got != "fake" {
		t.Errorf("result text = %q, want %q", got, "fake")
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("upstream echo called %d times, want 0", n)
	}
}

func TestFirstTerminalDecisionWins(t *testing.T) {
	var thirdCalls atomic.Int32
	var secondSaw atomic.Value
	hooks := []switchyard.Hook{
		onTool("first", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
			return switchyard.ContinueWith(json.RawMessage(`{"name":"echo","arguments":{"text":"rewritten"}}`))
		}),
		onTool("second", "echo", func(_ context.Context, r *switchyard.Request) switchyard.Decision {
			secondSaw.Store(string(r.Params))
			return switchyard.Block(&jsonrpc.Error{Code: -32001, Message: "blocked by second"})
		}),
		onTool("third", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
			thirdCalls.Add(1)
			return switchyard.Respond(json.RawMessage(`{"content":[{"type":"text","text":"third"}]}`))
		}),
	}
	cs := testkit.Connect(t, testkit.Server(t, echoTool(), slowTool()), switchyard.Options{Hooks: hooks})
	_, err := callEcho(t, cs, "hi")
	if err == nil || !strings.Contains(err.Error(), "blocked by second") {
		t.Fatalf("CallTool error = %v, want one containing %q", err, "blocked by second")
	}
	if n := thirdCalls.Load(); n != 0 {
		t.Errorf("third hook called %d times, want 0", n)
	}
	if got, _ := secondSaw.Load().(string); !strings.Contains(got, "rewritten") {
		t.Errorf("second hook saw params %s, want the first hook's rewrite", got)
	}
}

func TestSlowCallDoesNotBlockOthers(t *testing.T) {
	entered := make(chan struct{})
	hook := onTool("sleepy", "slow", func(context.Context, *switchyard.Request) switchyard.Decision {
		close(entered)
		time.Sleep(500 * time.Millisecond)
		return switchyard.Continue()
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool(), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})

	done := make(chan string, 2)
	go func() {
		if _, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "slow"}); err != nil {
			t.Errorf("CallTool slow: %v", err)
		}
		done <- "slow"
	}()
	<-entered
	go func() {
		if _, err := callEcho(t, cs, "hi"); err != nil {
			t.Errorf("CallTool echo: %v", err)
		}
		done <- "echo"
	}()
	if first := <-done; first != "echo" {
		t.Errorf("first call to return = %q, want echo", first)
	}
	<-done
}

func TestHangThenCancel(t *testing.T) {
	hookCtx := make(chan context.Context, 1)
	hook := onTool("hang", "echo", func(ctx context.Context, _ *switchyard.Request) switchyard.Decision {
		hookCtx <- ctx
		return switchyard.Hang()
	})
	cs, p := connectWithProxy(t, testkit.Server(t, echoTool(), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}}, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hi"}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool error = %v, want context.DeadlineExceeded", err)
	}
	waitFor(t, time.Second, "pending count to reach 0", func() bool { return switchyard.PendingLen(p) == 0 })
	select {
	case <-(<-hookCtx).Done():
	case <-time.After(time.Second):
		t.Fatal("hook context not cancelled after the client cancelled")
	}
}

func TestCancelDuringHookDropsRequest(t *testing.T) {
	var calls atomic.Int32
	hook := onTool("wait", "echo", func(ctx context.Context, _ *switchyard.Request) switchyard.Decision {
		<-ctx.Done()
		return switchyard.Continue()
	})
	cs, p := connectWithProxy(t, testkit.Server(t, countedEcho(&calls), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}}, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool error = %v, want context.DeadlineExceeded", err)
	}
	waitFor(t, time.Second, "pending count to reach 0", func() bool { return switchyard.PendingLen(p) == 0 })
	// A following call proves the cancelled one was not forwarded late.
	if _, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "slow"}); err != nil {
		t.Fatalf("CallTool slow: %v", err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("upstream echo called %d times after cancel, want 0", n)
	}
}

func TestHookPanic(t *testing.T) {
	var panicked atomic.Bool
	hook := onTool("boom", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		if !panicked.Swap(true) {
			panic("kaboom")
		}
		return switchyard.Continue()
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool(), slowTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	_, err := callEcho(t, cs, "hi")
	if err == nil || !strings.Contains(err.Error(), "hook boom panicked") {
		t.Fatalf("CallTool error = %v, want one containing %q", err, "hook boom panicked")
	}
	res, err := callEcho(t, cs, "again")
	if err != nil {
		t.Fatalf("second CallTool: %v", err)
	}
	if got := resultText(t, res); got != "again" {
		t.Errorf("result text = %q, want %q", got, "again")
	}
}

func TestNonToolRequestsOrdered(t *testing.T) {
	for _, tc := range []struct {
		name, protocol, handshake string
	}{
		{"legacy", "2025-11-25", "initialize"}, // then notifications/initialized
		{"modern", "", "server/discover"},      // SEP-2575, the SDK default
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var methods []string
			hook := &funcHook{name: "record", fn: func(_ context.Context, _ *switchyard.Session, r *switchyard.Request) switchyard.Decision {
				mu.Lock()
				methods = append(methods, r.Method)
				mu.Unlock()
				return switchyard.Continue()
			}}
			cs, _ := connectWithProxy(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}},
				&mcp.ClientSessionOptions{ProtocolVersion: tc.protocol})
			if cs.InitializeResult() == nil {
				t.Fatal("client did not initialize")
			}
			if _, err := cs.ListTools(t.Context(), nil); err != nil {
				t.Fatalf("ListTools: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(methods) < 2 || methods[0] != tc.handshake {
				t.Fatalf("hooked methods = %v, want %s first", methods, tc.handshake)
			}
			for _, m := range methods {
				if strings.HasPrefix(m, "notifications/") {
					t.Errorf("hook saw notification %q; only requests are hooked", m)
				}
			}
			if methods[len(methods)-1] != "tools/list" {
				t.Errorf("hooked methods = %v, want tools/list last", methods)
			}
		})
	}
}

// TestBaseHookForwardsOriginalBytes checks that a hook that continues leaves
// the request bytes untouched.
func TestBaseHookForwardsOriginalBytes(t *testing.T) {
	lines := []string{
		`{"params":{"b":2,"a":1},  "method":"tools/call","id":7,"jsonrpc":"2.0"}`,
		`{"id":8,"jsonrpc":"2.0","method":"ping"}`,
	}
	h := &funcHook{name: "base", fn: switchyard.Base{}.OnRequest}
	got := serveLines(t, switchyard.Options{Hooks: []switchyard.Hook{h}}, lines)
	// tools/call is hooked on its own goroutine, so order may differ.
	gotLines := strings.Split(strings.TrimSuffix(got.up, "\n"), "\n")
	slices.Sort(gotLines)
	want := slices.Sorted(slices.Values(lines))
	if !slices.Equal(gotLines, want) {
		t.Errorf("upstream got:\n%s\nwant (any order):\n%s", got.up, strings.Join(lines, "\n"))
	}
}

func TestBlockToolResultJSON(t *testing.T) {
	h := &funcHook{name: "deny", fn: func(context.Context, *switchyard.Session, *switchyard.Request) switchyard.Decision {
		return switchyard.BlockTool("nope")
	}}
	got := serveLines(t, switchyard.Options{Hooks: []switchyard.Hook{h}},
		[]string{`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}`})
	want := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"nope"}],"isError":true}}` + "\n"
	if got.down != want {
		t.Errorf("client got %s, want %s", got.down, want)
	}
	if got.up != "" {
		t.Errorf("upstream got %q, want nothing", got.up)
	}
}

func TestContinueWithRewritesOnlyParams(t *testing.T) {
	h := &funcHook{name: "rw", fn: func(context.Context, *switchyard.Session, *switchyard.Request) switchyard.Decision {
		return switchyard.ContinueWith(json.RawMessage(`{"name":"y"}`))
	}}
	got := serveLines(t, switchyard.Options{Hooks: []switchyard.Hook{h}},
		[]string{`{"jsonrpc":"2.0","id":"a","method":"tools/call","params":{"name":"x"},"extra":true}`})
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got.up), &m); err != nil {
		t.Fatalf("upstream got invalid JSON %q: %v", got.up, err)
	}
	for k, want := range map[string]string{"jsonrpc": `"2.0"`, "id": `"a"`, "method": `"tools/call"`, "params": `{"name":"y"}`, "extra": `true`} {
		if string(m[k]) != want {
			t.Errorf("upstream %s = %s, want %s", k, m[k], want)
		}
	}
}

func TestDecisionString(t *testing.T) {
	for _, tc := range []struct {
		d    switchyard.Decision
		want string
	}{
		{switchyard.Continue(), "continue"},
		{switchyard.Decision{}, "continue"},
		{switchyard.ContinueWith(json.RawMessage(`{}`)), "continue-with"},
		{switchyard.Block(&jsonrpc.Error{Code: 1, Message: "x"}), "block"},
		{switchyard.BlockTool("x"), "block-tool"},
		{switchyard.Respond(json.RawMessage(`{}`)), "respond"},
		{switchyard.Hang(), "hang"},
	} {
		if got := tc.d.String(); got != tc.want {
			t.Errorf("String() = %q, want %q", got, tc.want)
		}
	}
}

func TestRequestToolCall(t *testing.T) {
	r := &switchyard.Request{Method: "tools/call", Params: json.RawMessage(`{"name":"echo","arguments":{"text":"hi"}}`)}
	tc, ok := r.ToolCall()
	if !ok || tc.Name != "echo" || string(tc.Arguments) != `{"text":"hi"}` {
		t.Errorf("ToolCall() = %+v, %v; want echo with arguments", tc, ok)
	}
	if _, ok := (&switchyard.Request{Method: "tools/list"}).ToolCall(); ok {
		t.Error("ToolCall() ok for tools/list, want false")
	}
}

// TestSessionEndWithStuckSyncHook checks that Serve returns when the session
// ends while a synchronously hooked request's hook ignores its context.
func TestSessionEndWithStuckSyncHook(t *testing.T) {
	for _, end := range []string{"cancel", "upstream-eof"} {
		t.Run(end, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			entered := make(chan struct{})
			h := &funcHook{name: "stuck", fn: func(context.Context, *switchyard.Session, *switchyard.Request) switchyard.Decision {
				close(entered)
				<-release
				return switchyard.Continue()
			}}
			downR, downW := io.Pipe()
			defer downW.Close()
			upR, upW := io.Pipe()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			served := make(chan error, 1)
			go func() {
				served <- switchyard.New(switchyard.Options{Hooks: []switchyard.Hook{h}}).Serve(ctx, downR, io.Discard, upR, nopWriteCloser{io.Discard})
			}()
			if _, err := io.WriteString(downW, `{"jsonrpc":"2.0","id":1,"method":"ping"}`+"\n"); err != nil {
				t.Fatal(err)
			}
			<-entered
			var want error = context.Canceled
			if end == "cancel" {
				cancel()
			} else {
				_ = upW.Close()
				want = switchyard.ErrUpstreamClosed
			}
			select {
			case err := <-served:
				if !errors.Is(err, want) {
					t.Errorf("Serve = %v, want %v", err, want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Serve did not return while a hook was stuck")
			}
		})
	}
}

// TestCancelUnblocksStuckClientWrite checks that cancelling ctx stops Serve
// even when a loop is blocked writing to a client that stopped reading.
func TestCancelUnblocksStuckClientWrite(t *testing.T) {
	downR, downW := io.Pipe()
	defer downW.Close()
	_, downOutW := io.Pipe() // nobody reads what the proxy sends the client
	upR, upW := io.Pipe()
	defer upW.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- switchyard.New(switchyard.Options{}).Serve(ctx, downR, downOutW, upR, nopWriteCloser{io.Discard})
	}()
	// upLoop reads this, then blocks writing it to the client.
	if _, err := io.WriteString(upW, `{"jsonrpc":"2.0","method":"notifications/message"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Serve = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after cancel while blocked writing to the client")
	}
}

// TestClientWriteErrorNotReportedOverEOF checks that a failed write to a
// client that went away is not reported in place of the client's EOF.
func TestClientWriteErrorNotReportedOverEOF(t *testing.T) {
	downR, downW := io.Pipe()
	downOutR, downOutW := io.Pipe()
	_ = downOutR.Close() // the client stopped reading: writes fail
	upR, upW := io.Pipe()
	upOutR, upOutW := io.Pipe()
	go func() { // the fake upstream exits when its stdin closes
		_, _ = io.Copy(io.Discard, upOutR)
		_ = upW.Close()
	}()
	served := make(chan error, 1)
	go func() {
		served <- switchyard.New(switchyard.Options{}).Serve(t.Context(), downR, downOutW, upR, upOutW)
	}()
	if _, err := io.WriteString(upW, `{"jsonrpc":"2.0","method":"notifications/message"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the client write fail first
	_ = downW.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve = %v, want nil (the client's EOF)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return")
	}
}

// TestUpClosedOnce checks Serve closes the upstream reader exactly once.
func TestUpClosedOnce(t *testing.T) {
	up := &countingReader{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() {
		served <- switchyard.New(switchyard.Options{}).Serve(ctx, errReader{}, io.Discard, up, nopWriteCloser{io.Discard})
	}()
	waitFor(t, time.Second, "up to be closed", func() bool { return up.closes.Load() > 0 })
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(up.release)
	<-served
	if n := up.closes.Load(); n != 1 {
		t.Errorf("up closed %d times, want 1", n)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("down read failed") }

// countingReader blocks reads until release is closed, then reports EOF.
type countingReader struct {
	release chan struct{}
	closes  atomic.Int32
}

func (r *countingReader) Read([]byte) (int, error) { <-r.release; return 0, io.EOF }
func (r *countingReader) Close() error             { r.closes.Add(1); return nil }

type serveResult struct{ up, down string }

// serveLines runs Serve over a fake upstream that records what it receives
// and closes its output at EOF. The client sends lines and then EOF.
func serveLines(t *testing.T, opts switchyard.Options, lines []string) serveResult {
	t.Helper()
	upOutR, upOutW := io.Pipe()
	upR, upW := io.Pipe()
	var up []byte
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		up, _ = io.ReadAll(upOutR)
		_ = upW.Close()
	}()
	var downOut syncBuffer
	err := switchyard.New(opts).Serve(t.Context(), strings.NewReader(strings.Join(lines, "\n")+"\n"), &downOut, upR, upOutW)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	<-upDone
	return serveResult{up: string(up), down: downOut.String()}
}

// syncBuffer is a strings.Builder safe for concurrent use.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// connectWithProxy is testkit.Connect with client session options, also
// returning the proxy so tests can inspect its pending table.
func connectWithProxy(t *testing.T, server *mcp.Server, opts switchyard.Options, sessOpts *mcp.ClientSessionOptions) (*mcp.ClientSession, *switchyard.Proxy) {
	t.Helper()
	c2pR, c2pW := io.Pipe()
	p2cR, p2cW := io.Pipe()
	p2sR, p2sW := io.Pipe()
	s2pR, s2pW := io.Pipe()
	ss, err := server.Connect(context.Background(), &mcp.IOTransport{Reader: p2sR, Writer: s2pW}, nil)
	if err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := switchyard.New(opts)
	done := make(chan error, 1)
	go func() {
		err := p.Serve(ctx, c2pR, p2cW, s2pR, p2sW)
		_ = p2cW.Close()
		done <- err
	}()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "v0"}, nil).Connect(context.Background(), &mcp.IOTransport{Reader: p2cR, Writer: c2pW}, sessOpts)
	if err != nil {
		cancel()
		t.Fatalf("client Connect: %v", err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, switchyard.ErrUpstreamClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after the client closed")
			cancel()
			<-done
		}
		cancel()
		_ = ss.Close()
		_ = ss.Wait()
	})
	return cs, p
}

// TestBlockInvalidErrorData checks that a Block whose error data is not
// valid JSON answers the client with -32603 naming the hook, rather than
// sending nothing, and that the session keeps working.
func TestBlockInvalidErrorData(t *testing.T) {
	var blocked atomic.Bool
	hook := onTool("bad", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		if !blocked.Swap(true) {
			return switchyard.Block(&jsonrpc.Error{Code: -32001, Message: "denied", Data: json.RawMessage(`{`)})
		}
		return switchyard.Continue()
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := cs.CallTool(ctx, echoCall("hi"))
	if err == nil || !strings.Contains(err.Error(), "hook bad returned an invalid error") {
		t.Fatalf("CallTool error = %v, want one containing %q", err, "hook bad returned an invalid error")
	}
	res, err := callEcho(t, cs, "again")
	if err != nil {
		t.Fatalf("second CallTool: %v", err)
	}
	if got := resultText(t, res); got != "again" {
		t.Errorf("result text = %q, want %q", got, "again")
	}
}
