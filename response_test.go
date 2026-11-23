package switchyard_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"regexp"
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

// fakeUpstream is a Serve session whose upstream is reply: every message the
// proxy sends upstream is passed to reply, and the lines it returns are sent
// back. The client side is driven with send and recv.
type fakeUpstream struct {
	t      *testing.T
	proxy  *switchyard.Proxy
	down   *io.PipeWriter
	out    *bufio.Reader
	gotUp  chan string
	served chan error
}

func newFakeUpstream(t *testing.T, opts switchyard.Options, reply func(m *jsonrpc.Message) []string) *fakeUpstream {
	t.Helper()
	downR, downW := io.Pipe()
	outR, outW := io.Pipe()
	upR, upW := io.Pipe()
	upOutR, upOutW := io.Pipe()
	f := &fakeUpstream{
		t: t, proxy: switchyard.New(opts), down: downW, out: bufio.NewReader(outR),
		gotUp: make(chan string, 100), served: make(chan error, 1),
	}
	go func() {
		defer upW.Close()
		r := jsonrpc.NewReader(upOutR)
		for {
			m, err := r.Read()
			if err != nil {
				return
			}
			f.gotUp <- string(m.Raw)
			for _, l := range reply(m) {
				if _, err := io.WriteString(upW, l+"\n"); err != nil {
					return
				}
			}
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := f.proxy.Serve(ctx, downR, outW, upR, upOutW)
		_ = outW.Close()
		f.served <- err
	}()
	t.Cleanup(func() {
		defer cancel()
		_ = downW.Close()
		go func() { _, _ = io.Copy(io.Discard, f.out) }()
		select {
		case err := <-f.served:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after the client closed")
		}
	})
	return f
}

func (f *fakeUpstream) send(line string) {
	f.t.Helper()
	if _, err := io.WriteString(f.down, line+"\n"); err != nil {
		f.t.Fatalf("writing to proxy: %v", err)
	}
}

// recv returns the next line the client receives, without its newline.
func (f *fakeUpstream) recv() string {
	f.t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		l, err := f.out.ReadString('\n')
		ch <- res{strings.TrimSuffix(l, "\n"), err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			f.t.Fatalf("reading from proxy: %v", r.err)
		}
		return r.line
	case <-time.After(5 * time.Second):
		f.t.Fatal("timed out waiting for the client to receive a message")
		return ""
	}
}

// replyResult answers every request with {"jsonrpc":"2.0","id":<id>,"result":result}.
func replyResult(result string) func(m *jsonrpc.Message) []string {
	return func(m *jsonrpc.Message) []string {
		if m.Kind() != jsonrpc.KindRequest {
			return nil
		}
		return []string{`{"jsonrpc":"2.0","id":` + string(m.ID) + `,"result":` + result + `}`}
	}
}

// TestContinueWithKeepsIDBytes checks that rewriting params leaves a string
// id with HTML-special characters byte-identical, so the response still
// matches its pending request.
func TestContinueWithKeepsIDBytes(t *testing.T) {
	h := &funcHook{name: "rw", fn: func(context.Context, *switchyard.Session, *switchyard.Request) switchyard.Decision {
		return switchyard.ContinueWith(json.RawMessage(`{"name":"y"}`))
	}}
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{h}}, replyResult(`{}`))
	f.send(`{"jsonrpc":"2.0","id":"<&>","method":"tools/call","params":{"name":"x"},"_meta":{"k":"<b>"}}`)
	if got, want := <-f.gotUp, `{"jsonrpc":"2.0","id":"<&>","method":"tools/call","params":{"name":"y"},"_meta":{"k":"<b>"}}`; got != want {
		t.Errorf("upstream got %s, want %s", got, want)
	}
	if got, want := f.recv(), `{"jsonrpc":"2.0","id":"<&>","result":{}}`; got != want {
		t.Errorf("client got %s, want %s", got, want)
	}
	waitFor(t, time.Second, "pending count to reach 0", func() bool { return switchyard.PendingLen(f.proxy) == 0 })
}

// respHook is a Hook whose OnResponse is a function; nil onReq continues.
type respHook struct {
	switchyard.Base
	name   string
	onReq  func(s *switchyard.Session, r *switchyard.Request) switchyard.Decision
	onResp func(s *switchyard.Session, r *switchyard.Request, resp *switchyard.Response) *switchyard.Response
}

func (h *respHook) Name() string { return h.name }

func (h *respHook) OnRequest(_ context.Context, s *switchyard.Session, r *switchyard.Request) switchyard.Decision {
	if h.onReq == nil {
		return switchyard.Continue()
	}
	return h.onReq(s, r)
}

func (h *respHook) OnResponse(_ context.Context, s *switchyard.Session, r *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
	return h.onResp(s, r, resp)
}

// appendText returns a hook appending its name to the first text content of
// every tools/call result.
func appendText(t *testing.T, name string) *respHook {
	return &respHook{name: name, onResp: func(_ *switchyard.Session, r *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		if r.Method != "tools/call" {
			return resp
		}
		tr, ok := resp.ToolResult()
		if !ok {
			t.Errorf("hook %s: ToolResult() not ok for %s", name, resp.Result)
			return resp
		}
		var c struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(tr.Content[0], &c); err != nil {
			t.Errorf("hook %s: decoding content: %v", name, err)
			return resp
		}
		c.Text += name
		tr.Content[0], _ = json.Marshal(c)
		return &switchyard.Response{Result: tr.Marshal()}
	}}
}

func TestOnResponseReverseOrder(t *testing.T) {
	cs := testkit.Connect(t, testkit.Server(t, echoTool()), switchyard.Options{Hooks: []switchyard.Hook{appendText(t, "A"), appendText(t, "B")}})
	res, err := callEcho(t, cs, "echo:")
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if got := resultText(t, res); got != "echo:BA" {
		t.Errorf("result text = %q, want %q", got, "echo:BA")
	}
}

func TestToolListRewrite(t *testing.T) {
	var sess atomic.Pointer[switchyard.Session]
	hook := &respHook{name: "hide", onResp: func(s *switchyard.Session, r *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		sess.Store(s)
		tools, ok := resp.ToolList()
		if r.Method != "tools/list" || !ok {
			return resp
		}
		kept := slices.DeleteFunc(tools, func(ti switchyard.ToolInfo) bool { return ti.Name == "secret" })
		return &switchyard.Response{Result: switchyard.ToolListResult(resp.Result, kept)}
	}}
	secret := echoTool()
	secret.Name = "secret"
	cs := testkit.Connect(t, testkit.Server(t, echoTool(), secret), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	list, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(list.Tools) != 1 || list.Tools[0].Name != "echo" || list.Tools[0].Description != "echoes text" {
		t.Fatalf("tools = %+v, want only echo, with its description", list.Tools)
	}
	got := sess.Load().Tools()
	if len(got) != 1 || got[0].Name != "echo" {
		t.Fatalf("Session.Tools() = %+v, want only echo", got)
	}
	var raw struct{ Description string }
	if err := json.Unmarshal(got[0].Raw, &raw); err != nil || raw.Description != "echoes text" {
		t.Errorf("Session.Tools()[0].Raw = %s, want the echo tool's JSON", got[0].Raw)
	}
}

func TestObserveResourcesRead(t *testing.T) {
	hook := &respHook{name: "observe", onResp: func(s *switchyard.Session, r *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		if r.Method == "resources/read" {
			var res struct {
				Contents []struct{ Text string } `json:"contents"`
			}
			if err := json.Unmarshal(resp.Result, &res); err == nil && len(res.Contents) == 1 {
				s.Set("read", res.Contents[0].Text)
			}
		}
		return resp
	}}
	var sess atomic.Pointer[switchyard.Session]
	hook.onReq = func(s *switchyard.Session, _ *switchyard.Request) switchyard.Decision {
		sess.Store(s)
		return switchyard.Continue()
	}
	resources := []testkit.ResourceSpec{{URI: "file:///a", Text: "hello"}}
	cs := testkit.Connect(t, testkit.ServerWithResources(t, resources), switchyard.Options{Hooks: []switchyard.Hook{hook}})
	direct := testkit.ConnectDirect(t, testkit.ServerWithResources(t, resources))
	params := &mcp.ReadResourceParams{URI: "file:///a"}
	got, err := cs.ReadResource(t.Context(), params)
	if err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	want, err := direct.ReadResource(t.Context(), params)
	if err != nil {
		t.Fatalf("direct ReadResource: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("result via proxy = %+v, direct = %+v", got, want)
	}
	if v, ok := sess.Load().Get("read"); !ok || v != "hello" {
		t.Errorf(`Session.Get("read") = %v, %v; want "hello", true`, v, ok)
	}
}

func TestToolResultRoundTripKeepsUnknownFields(t *testing.T) {
	in := `{"content":[{"type":"text","text":"<hi>"}],"isError":true,"structuredContent":{"a":"<&>"},"_meta":{"k":1}}`
	tr, ok := (&switchyard.Response{Result: json.RawMessage(in)}).ToolResult()
	if !ok {
		t.Fatal("ToolResult() not ok")
	}
	if len(tr.Content) != 1 || !tr.IsError || string(tr.Rest["structuredContent"]) != `{"a":"<&>"}` {
		t.Fatalf("ToolResult() = %+v", tr)
	}
	out := tr.Marshal()
	if strings.Contains(string(out), `\u00`) {
		t.Errorf("Marshal() = %s, want no HTML escaping", out)
	}
	var got, want any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("Marshal() = %s: %v", out, err)
	}
	_ = json.Unmarshal([]byte(in), &want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Marshal() = %s, want equivalent to %s", out, in)
	}
	tr.IsError = false
	if out := string(tr.Marshal()); strings.Contains(out, "isError") {
		t.Errorf("Marshal() with IsError false = %s, want no isError", out)
	}
}

func TestToolResultRejectsNonToolResults(t *testing.T) {
	for _, resp := range []*switchyard.Response{
		{Error: &jsonrpc.Error{Code: 1, Message: "x"}},
		{Result: json.RawMessage(`[]`)},
		{Result: json.RawMessage(`{"tools":[]}`)},
		{Result: json.RawMessage(`{"content":{}}`)},
	} {
		if tr, ok := resp.ToolResult(); ok {
			t.Errorf("ToolResult() for %s = %+v, want not ok", resp.Result, tr)
		}
	}
}

func TestToolListResultKeepsOtherFields(t *testing.T) {
	orig := json.RawMessage(`{"tools":[{"name":"a"},{"name":"b","description":"<b>"}], "nextCursor":"<c>"}`)
	tools, ok := (&switchyard.Response{Result: orig}).ToolList()
	if !ok || len(tools) != 2 || tools[1].Name != "b" || string(tools[1].Raw) != `{"name":"b","description":"<b>"}` {
		t.Fatalf("ToolList() = %+v, %v", tools, ok)
	}
	got := string(switchyard.ToolListResult(orig, tools[1:]))
	if want := `{"tools":[{"name":"b","description":"<b>"}], "nextCursor":"<c>"}`; got != want {
		t.Errorf("ToolListResult = %s, want %s", got, want)
	}
	got = string(switchyard.ToolListResult(orig, nil))
	if want := `{"tools":[], "nextCursor":"<c>"}`; got != want {
		t.Errorf("ToolListResult(nil) = %s, want %s", got, want)
	}
	if _, ok := (&switchyard.Response{Result: json.RawMessage(`{"content":[]}`)}).ToolList(); ok {
		t.Error("ToolList() ok for a tools/call result, want false")
	}
}

func TestSessionUpdateConcurrent(t *testing.T) {
	var sess atomic.Pointer[switchyard.Session]
	h := &respHook{name: "capture", onReq: func(s *switchyard.Session, _ *switchyard.Request) switchyard.Decision {
		sess.Store(s)
		return switchyard.Continue()
	}, onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		return resp
	}}
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{h}}, replyResult(`{}`))
	f.send(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	f.recv()
	s := sess.Load()
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			s.Update("n", func(old any) any {
				n, _ := old.(int)
				return n + 1
			})
		})
	}
	wg.Wait()
	if v, ok := s.Get("n"); !ok || v != 100 {
		t.Errorf(`Get("n") = %v, %v; want 100, true`, v, ok)
	}
	if got := s.Update("n", func(old any) any { return old.(int) * 2 }); got != 200 {
		t.Errorf("Update returned %v, want 200", got)
	}
	s.Set("k", "v")
	if v, ok := s.Get("k"); !ok || v != "v" {
		t.Errorf(`Get("k") = %v, %v; want "v", true`, v, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error(`Get("missing") ok, want false`)
	}
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(s.ID()) {
		t.Errorf("ID() = %q, want 16 hex characters", s.ID())
	}
}

// TestUnchangedResponseBytesIdentical checks that a response no hook changed
// reaches the client as the upstream's original bytes.
func TestUnchangedResponseBytesIdentical(t *testing.T) {
	var calls atomic.Int32
	h := &respHook{name: "base", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		calls.Add(1)
		return resp
	}}
	weird := func(m *jsonrpc.Message) []string {
		return []string{`{"result":{"z":"<&>", "a":[ ]},  "id":` + string(m.ID) + `,"jsonrpc":"2.0"}`}
	}
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{h}}, weird)
	f.send(`{"jsonrpc":"2.0","id":"<1>","method":"tools/call","params":{"name":"x"}}`)
	if got, want := f.recv(), `{"result":{"z":"<&>", "a":[ ]},  "id":"<1>","jsonrpc":"2.0"}`; got != want {
		t.Errorf("client got %s, want %s", got, want)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("OnResponse called %d times, want 1", n)
	}
	waitFor(t, time.Second, "pending count to reach 0", func() bool { return switchyard.PendingLen(f.proxy) == 0 })
}

// TestOnResponseOnlyForForwarded checks that OnResponse does not run for
// requests a hook answered or held.
func TestOnResponseOnlyForForwarded(t *testing.T) {
	var calls atomic.Int32
	observe := &respHook{name: "observe", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		calls.Add(1)
		return resp
	}}
	decide := &funcHook{name: "decide", fn: func(_ context.Context, _ *switchyard.Session, r *switchyard.Request) switchyard.Decision {
		switch r.Method {
		case "block":
			return switchyard.Block(nil)
		case "blocktool":
			return switchyard.BlockTool("no")
		case "respond":
			return switchyard.Respond(json.RawMessage(`{}`))
		case "hang":
			return switchyard.Hang()
		}
		return switchyard.Continue()
	}}
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{observe, decide}}, replyResult(`{}`))
	f.send(`{"jsonrpc":"2.0","id":9,"method":"hang"}`)
	for i, m := range []string{"block", "blocktool", "respond"} {
		f.send(`{"jsonrpc":"2.0","id":` + string(rune('1'+i)) + `,"method":"` + m + `"}`)
		f.recv()
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("OnResponse called %d times for unforwarded requests, want 0", n)
	}
	f.send(`{"jsonrpc":"2.0","id":5,"method":"ping"}`)
	f.recv()
	if n := calls.Load(); n != 1 {
		t.Errorf("OnResponse called %d times after a forwarded request, want 1", n)
	}
}

func TestOnResponsePanic(t *testing.T) {
	var panicked atomic.Bool
	later := &respHook{name: "later", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		return &switchyard.Response{Result: json.RawMessage(`{"changed":true}`)}
	}}
	boom := &respHook{name: "boom", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		if !panicked.Swap(true) {
			panic("kaboom")
		}
		return resp
	}}
	// OnResponse runs in reverse order: later, then boom.
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{boom, later}}, replyResult(`{}`))
	f.send(`{"jsonrpc":"2.0","id":"<&>","method":"tools/call","params":{"name":"x"}}`)
	if got, want := f.recv(), `{"jsonrpc":"2.0","id":"<&>","error":{"code":-32603,"message":"hook boom panicked"}}`; got != want {
		t.Errorf("client got %s, want %s", got, want)
	}
	f.send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"x"}}`)
	if got, want := f.recv(), `{"jsonrpc":"2.0","id":2,"result":{"changed":true}}`; got != want {
		t.Errorf("client got %s, want %s", got, want)
	}
	waitFor(t, time.Second, "pending count to reach 0", func() bool { return switchyard.PendingLen(f.proxy) == 0 })
}

func TestOnResponseInvalidResult(t *testing.T) {
	h := &respHook{name: "bad", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		return &switchyard.Response{Result: json.RawMessage(`{nope`)}
	}}
	f := newFakeUpstream(t, switchyard.Options{Hooks: []switchyard.Hook{h}}, replyResult(`{}`))
	f.send(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	if got, want := f.recv(), `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"hook bad returned an invalid result"}}`; got != want {
		t.Errorf("client got %s, want %s", got, want)
	}
}

// TestStuckOnResponseDoesNotBlockCancel checks that Serve returns on ctx
// cancellation while an OnResponse hook ignores its context.
func TestStuckOnResponseDoesNotBlockCancel(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{})
	h := &respHook{name: "stuck", onResp: func(_ *switchyard.Session, _ *switchyard.Request, resp *switchyard.Response) *switchyard.Response {
		close(entered)
		<-release
		return resp
	}}
	downR, downW := io.Pipe()
	defer downW.Close()
	upR, upW := io.Pipe()
	defer upW.Close()
	upOutR, upOutW := io.Pipe()
	go func() { // answer the one request
		r := jsonrpc.NewReader(upOutR)
		if m, err := r.Read(); err == nil {
			_, _ = io.WriteString(upW, `{"jsonrpc":"2.0","id":`+string(m.ID)+`,"result":{}}`+"\n")
		}
		_, _ = io.Copy(io.Discard, upOutR)
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- switchyard.New(switchyard.Options{Hooks: []switchyard.Hook{h}}).Serve(ctx, downR, io.Discard, upR, upOutW)
	}()
	if _, err := io.WriteString(downW, `{"jsonrpc":"2.0","id":1,"method":"ping"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after cancel while OnResponse was stuck")
	}
}
