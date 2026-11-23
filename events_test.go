package switchyard_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sadayamuthu/switchyard"
	"github.com/sadayamuthu/switchyard/testkit"
)

func echoCall(text string) *mcp.CallToolParams {
	return &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": text}}
}

func TestEventsForToolCall(t *testing.T) {
	rec := &testkit.Recorder{}
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{
		Name: "hop-a", ChainID: "chain-1", Sink: rec,
		Hooks: []switchyard.Hook{&funcHook{name: "noop", fn: func(context.Context, *switchyard.Session, *switchyard.Request) switchyard.Decision {
			return switchyard.Continue()
		}}},
	})
	if _, err := cs.CallTool(context.Background(), echoCall("hi")); err != nil {
		t.Fatal(err)
	}
	var calls []switchyard.Event
	for _, e := range rec.Events() {
		if e.Method == "tools/call" {
			calls = append(calls, e)
		}
	}
	if len(calls) != 2 {
		t.Fatalf("got %d tools/call events, want 2: %+v", len(calls), calls)
	}
	req, resp := calls[0], calls[1]
	if req.Kind != "request" || resp.Kind != "response" {
		t.Fatalf("kinds = %q, %q", req.Kind, resp.Kind)
	}
	if req.Decision != "continue" || req.Tool != "echo" || req.Hook != "" {
		t.Errorf("request event = %+v", req)
	}
	if len(resp.Result) == 0 || resp.Error != nil || resp.LatencyMS <= 0 {
		t.Errorf("response event = %+v", resp)
	}
	for _, e := range calls {
		if e.V != 1 || e.Chain != "chain-1" || e.Hop != "hop-a" || e.Session == "" || e.ID == "" || e.Time.IsZero() {
			t.Errorf("event = %+v", e)
		}
	}
	if req.ID != resp.ID || req.Session != resp.Session {
		t.Errorf("id/session differ: %+v %+v", req, resp)
	}
}

func TestEventDecisionAnnotated(t *testing.T) {
	rec := &testkit.Recorder{}
	hang := onTool("chaos", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.Hang().Annotate("fault-timeout")
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Name: "h", Hooks: []switchyard.Hook{hang}, Sink: rec})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = cs.CallTool(ctx, echoCall("x"))
	var got *switchyard.Event
	for _, e := range rec.Events() {
		if e.Kind == "request" && e.Tool == "echo" {
			got = &e
		}
		if e.Kind == "response" && e.Method == "tools/call" {
			t.Errorf("unexpected response event for hung request: %+v", e)
		}
	}
	if got == nil || got.Decision != "hang:fault-timeout" || got.Hook != "chaos" {
		t.Fatalf("request event = %+v", got)
	}
	if switchyard.Hang().String() != "hang" {
		t.Error("Annotate must not change the original")
	}
}

func TestEventBlockedResponse(t *testing.T) {
	rec := &testkit.Recorder{}
	h := onTool("deny", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.BlockTool("nope")
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{h}, Sink: rec})
	if _, err := cs.CallTool(context.Background(), echoCall("x")); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, e := range rec.Events() {
		if e.Method != "tools/call" {
			continue
		}
		kinds = append(kinds, e.Kind+"/"+e.Decision)
		if e.Kind == "request" && e.Hook != "deny" {
			t.Errorf("hook = %q", e.Hook)
		}
		if e.Kind == "response" && !strings.Contains(string(e.Result), "nope") {
			t.Errorf("result = %s", e.Result)
		}
	}
	if strings.Join(kinds, ",") != "request/block-tool,response/" {
		t.Errorf("kinds = %v", kinds)
	}
}

func TestEventsRewrittenParamsAndNoSink(t *testing.T) {
	rec := &testkit.Recorder{}
	h := onTool("rw", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		return switchyard.ContinueWith(json.RawMessage(`{"name":"echo","arguments":{"text":"changed"}}`))
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{h}, Sink: rec})
	if _, err := cs.CallTool(context.Background(), echoCall("orig")); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range rec.Events() {
		if e.Kind == "request" && e.Tool == "echo" {
			found = true
			if e.Decision != "continue-with" || !strings.Contains(string(e.Params), "changed") {
				t.Errorf("event = %+v", e)
			}
		}
	}
	if !found {
		t.Fatal("no request event")
	}
	// A nil sink must work.
	cs2 := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{h}})
	if _, err := cs2.CallTool(context.Background(), echoCall("orig")); err != nil {
		t.Fatal(err)
	}
}

type panicSink struct{}

func (panicSink) Emit(switchyard.Event) { panic("sink") }

func TestPanickingSinkDoesNotCrashProxy(t *testing.T) {
	h := onTool("x", "none", nil)
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{h}, Sink: panicSink{}})
	if _, err := cs.CallTool(context.Background(), echoCall("hi")); err != nil {
		t.Fatal(err)
	}
}

func TestJSONLSink(t *testing.T) {
	var buf bytes.Buffer
	s := switchyard.NewJSONLSink(&buf)
	s.Emit(switchyard.Event{V: switchyard.EventSchemaVersion, Kind: "request", Method: "tools/call", Tool: "a<b"})
	s.Emit(switchyard.Event{V: switchyard.EventSchemaVersion, Kind: "response", Method: "tools/call"})
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines: %q", len(lines), buf.String())
	}
	for _, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("line %q: %v", l, err)
		}
		if !strings.Contains(l, `"v":1`) || m["v"] != float64(1) {
			t.Errorf("line %q lacks v:1", l)
		}
	}
	if !strings.Contains(lines[0], "a<b") {
		t.Errorf("HTML was escaped: %s", lines[0])
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, bytes.ErrTooLarge }

func TestJSONLSinkDropsOnWriteError(t *testing.T) {
	switchyard.NewJSONLSink(failWriter{}).Emit(switchyard.Event{V: 1})
}

func TestChainIDFromEnv(t *testing.T) {
	t.Setenv(switchyard.ChainEnv, "abc123")
	if got := switchyard.ChainIDFromEnv(); got != "abc123" {
		t.Errorf("got %q", got)
	}
	t.Setenv(switchyard.ChainEnv, "")
	a, b := switchyard.ChainIDFromEnv(), switchyard.ChainIDFromEnv()
	re := regexp.MustCompile(`^[0-9a-f]{16}$`)
	if !re.MatchString(a) || !re.MatchString(b) || a == b {
		t.Errorf("got %q and %q", a, b)
	}
	if switchyard.ChainEnv != "SWITCHYARD_CHAIN_ID" {
		t.Errorf("ChainEnv = %q", switchyard.ChainEnv)
	}
}

func requestEvent(t *testing.T, rec *testkit.Recorder) switchyard.Event {
	t.Helper()
	for _, e := range rec.Events() {
		if e.Kind == "request" && e.Tool == "echo" {
			return e
		}
	}
	t.Fatal("no request event")
	return switchyard.Event{}
}

func TestEventDecisionLabels(t *testing.T) {
	contWith := json.RawMessage(`{"name":"echo","arguments":{"text":"y"}}`)
	for _, tc := range []struct {
		name  string
		hooks []switchyard.Hook
		want  string
	}{
		{"continue", []switchyard.Hook{onTool("a", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
			return switchyard.Continue().Annotate("fault-x")
		})}, "continue:fault-x"},
		{"continue-with", []switchyard.Hook{onTool("a", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
			return switchyard.ContinueWith(contWith).Annotate("fault-corrupt")
		})}, "continue-with:fault-corrupt"},
		{"two hooks", []switchyard.Hook{
			onTool("a", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
				return switchyard.Continue().Annotate("fault-slow")
			}),
			onTool("b", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
				return switchyard.Continue().Annotate("audit")
			}),
		}, "continue:fault-slow,audit"},
		{"terminal keeps earlier", []switchyard.Hook{
			onTool("a", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
				return switchyard.Continue().Annotate("fault-slow")
			}),
			onTool("b", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
				return switchyard.BlockTool("no").Annotate("deny")
			}),
		}, "block-tool:fault-slow,deny"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &testkit.Recorder{}
			cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: tc.hooks, Sink: rec})
			if _, err := cs.CallTool(context.Background(), echoCall("x")); err != nil {
				t.Fatal(err)
			}
			if got := requestEvent(t, rec).Decision; got != tc.want {
				t.Errorf("decision = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRequestEventTimeIsReceiptTime(t *testing.T) {
	rec := &testkit.Recorder{}
	var returned time.Time
	h := onTool("slow", "echo", func(context.Context, *switchyard.Request) switchyard.Decision {
		time.Sleep(200 * time.Millisecond)
		returned = time.Now()
		return switchyard.Continue()
	})
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{h}, Sink: rec})
	before := time.Now()
	if _, err := cs.CallTool(context.Background(), echoCall("x")); err != nil {
		t.Fatal(err)
	}
	req := requestEvent(t, rec)
	if req.Time.Before(before) || !req.Time.Before(returned) {
		t.Errorf("request Time %v not in [%v, hook returned %v)", req.Time, before, returned)
	}
}

// TestNoEventAfterServeReturns checks that a tools/call hook that ignores its
// context and decides after Serve has returned emits nothing, and that no
// request event follows its own response event.
func TestNoEventAfterServeReturns(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	h := onTool("stuck", "x", func(context.Context, *switchyard.Request) switchyard.Decision {
		close(entered)
		<-release
		return switchyard.Continue()
	})
	rec := &testkit.Recorder{}
	downR, downW := io.Pipe()
	defer downW.Close()
	upR, upW := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- switchyard.New(switchyard.Options{Hooks: []switchyard.Hook{h}, Sink: rec}).
			Serve(t.Context(), downR, io.Discard, upR, nopWriteCloser{io.Discard})
	}()
	if _, err := io.WriteString(downW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	_ = upW.Close()
	select {
	case err := <-served:
		if !errors.Is(err, switchyard.ErrUpstreamClosed) {
			t.Fatalf("Serve = %v, want ErrUpstreamClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return")
	}
	before := rec.Events()
	close(release)
	time.Sleep(100 * time.Millisecond) // let the hook's goroutine finish
	if after := rec.Events(); len(after) != len(before) {
		t.Errorf("events after Serve returned: %+v", after[len(before):])
	}
	sawResponse := false
	for _, e := range rec.Events() {
		switch e.Kind {
		case switchyard.KindResponse:
			sawResponse = true
		case switchyard.KindRequest:
			if sawResponse {
				t.Errorf("request event after its response event: %+v", e)
			}
		}
	}
}

// TestChainIDDefaulted checks that a proxy with no Options.ChainID uses
// ChainIDFromEnv, generating one 16-hex-character ID kept for every session.
func TestChainIDDefaulted(t *testing.T) {
	t.Setenv(switchyard.ChainEnv, "")
	rec := &testkit.Recorder{}
	opts := switchyard.Options{Hooks: []switchyard.Hook{&funcHook{name: "noop", fn: switchyard.Base{}.OnRequest}}, Sink: rec}
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), opts)
	if _, err := cs.CallTool(context.Background(), echoCall("x")); err != nil {
		t.Fatal(err)
	}
	chain := requestEvent(t, rec).Chain
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(chain) {
		t.Errorf("chain = %q, want 16 hex characters", chain)
	}

	// One proxy keeps its chain ID across sessions.
	rec2 := &testkit.Recorder{}
	opts.Sink = rec2
	p := switchyard.New(opts)
	for range 2 {
		in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n")
		if err := p.Serve(t.Context(), in, io.Discard, strings.NewReader(""), io.Discard); err != nil && !errors.Is(err, switchyard.ErrUpstreamClosed) {
			t.Fatalf("Serve: %v", err)
		}
	}
	evs := rec2.Events()
	if len(evs) < 2 {
		t.Fatalf("got %d events, want one per session at least: %+v", len(evs), evs)
	}
	for _, e := range evs {
		if e.Chain != evs[0].Chain || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(e.Chain) {
			t.Errorf("event chain %q, want one 16-hex ID %q for the proxy", e.Chain, evs[0].Chain)
		}
	}
}
