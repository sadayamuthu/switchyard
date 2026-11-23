package switchyard_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sadayamuthu/switchyard"
	"github.com/sadayamuthu/switchyard/jsonrpc"
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
