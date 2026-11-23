package switchyard_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sadayamuthu/switchyard"
)

// stuckReader returns data, then blocks in Read until the test ends, like a
// blocking-mode os.Stdin with nothing more to read. It has no Close method,
// so nothing the proxy does can unblock it.
type stuckReader struct {
	data    *strings.Reader
	release chan struct{}
}

func newStuckReader(t *testing.T, data string) *stuckReader {
	r := &stuckReader{data: strings.NewReader(data), release: make(chan struct{})}
	t.Cleanup(func() { close(r.release) })
	return r
}

func (r *stuckReader) Read(p []byte) (int, error) {
	if r.data.Len() > 0 {
		return r.data.Read(p)
	}
	<-r.release
	return 0, io.EOF
}

// runCommand runs RunCommand on its own goroutine and returns its result, or
// fails the test if it takes longer than limit.
func runCommand(t *testing.T, ctx context.Context, p *switchyard.Proxy, argv []string, down io.Reader, downOut io.Writer, limit time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- switchyard.RunCommand(ctx, p, argv, down, downOut) }()
	select {
	case err := <-done:
		return time.Since(start), err
	case <-time.After(limit):
		t.Fatalf("RunCommand(%q) did not return within %v", argv, limit)
		return 0, nil
	}
}

func TestUpstreamCommandMissing(t *testing.T) {
	const bin = "switchyard-no-such-binary-e2e"
	p := switchyard.New(switchyard.Options{Name: "test"})
	_, err := runCommand(t, t.Context(), p, []string{bin, "--flag"}, strings.NewReader(""), io.Discard, 5*time.Second)
	if err == nil {
		t.Fatal("RunCommand with a missing binary returned nil")
	}
	if !strings.Contains(err.Error(), bin) {
		t.Errorf("error %q does not name %q", err, bin)
	}
}

func TestRunCommandNoArgv(t *testing.T) {
	p := switchyard.New(switchyard.Options{})
	_, err := runCommand(t, t.Context(), p, nil, strings.NewReader(""), io.Discard, 5*time.Second)
	if err == nil {
		t.Fatal("RunCommand with no command returned nil")
	}
}

// The controller's ruling: with a downstream reader that never returns and
// cannot be closed, RunCommand still returns soon after the upstream exits,
// and the client has been told its pending request failed.
func TestRunCommandReturnsWhenUpstreamExitsDespiteStuckDown(t *testing.T) {
	down := newStuckReader(t, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x"}}`+"\n")
	out := &syncBuffer{}
	p := switchyard.New(switchyard.Options{Name: "test"})
	// The upstream reads the request, then exits without answering.
	argv := []string{"sh", "-c", "head -n 1 >/dev/null"}
	_, err := runCommand(t, t.Context(), p, argv, down, out, 3*time.Second)
	if !errors.Is(err, switchyard.ErrUpstreamClosed) {
		t.Errorf("RunCommand error = %v, want ErrUpstreamClosed", err)
	}
	const want = `{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"upstream closed"}}`
	if got := out.String(); !strings.Contains(got, want) {
		t.Errorf("client got %q, want it to contain %q", got, want)
	}
}

// An upstream that exits while a background process it started still holds
// its stdout must not keep RunCommand from returning.
func TestRunCommandReturnsWhenUpstreamExitsLeavingStdoutOpen(t *testing.T) {
	down := newStuckReader(t, "")
	p := switchyard.New(switchyard.Options{Name: "test"})
	// The background sleep holds the upstream's stdout open after it exits;
	// it records its pid so that cleanup can kill it.
	pidFile := filepath.Join(t.TempDir(), "pid")
	t.Cleanup(func() {
		b, err := os.ReadFile(pidFile)
		if err != nil {
			t.Errorf("reading the background pid: %v", err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			t.Errorf("bad background pid %q: %v", b, err)
			return
		}
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
	argv := []string{"sh", "-c", `sleep 5 </dev/null 2>/dev/null & echo $! >"$1"; exit 0`, "sh", pidFile}
	_, err := runCommand(t, t.Context(), p, argv, down, io.Discard, 4*time.Second)
	if !errors.Is(err, switchyard.ErrUpstreamClosed) {
		t.Errorf("RunCommand error = %v, want ErrUpstreamClosed", err)
	}
}

func TestRunCommandKillsUpstreamIgnoringStdinEOF(t *testing.T) {
	p := switchyard.New(switchyard.Options{Name: "test"})
	took, err := runCommand(t, t.Context(), p, []string{"sleep", "30"}, strings.NewReader(""), io.Discard, 5*time.Second)
	if err != nil {
		t.Errorf("RunCommand error = %v, want nil after downstream EOF", err)
	}
	if took < 1500*time.Millisecond {
		t.Errorf("RunCommand returned after %v; the upstream should get 2s to exit", took)
	}
}

func TestRunCommandCancelKillsUpstream(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(100*time.Millisecond, cancel)
	p := switchyard.New(switchyard.Options{Name: "test"})
	_, err := runCommand(t, ctx, p, []string{"sleep", "30"}, newStuckReader(t, ""), io.Discard, 2*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("RunCommand error = %v, want context.Canceled", err)
	}
}

// printChainEnv is an upstream that sends the client one notification
// carrying the chain ID it sees, then exits.
var printChainEnv = []string{"sh", "-c", `printf '{"jsonrpc":"2.0","method":"chain","params":{"id":"%s"}}\n' "$` + switchyard.ChainEnv + `"`}

func chainSeenByUpstream(t *testing.T, opts switchyard.Options) string {
	t.Helper()
	out := &syncBuffer{}
	_, err := runCommand(t, t.Context(), switchyard.New(opts), printChainEnv, newStuckReader(t, ""), out, 3*time.Second)
	if !errors.Is(err, switchyard.ErrUpstreamClosed) {
		t.Errorf("RunCommand error = %v, want ErrUpstreamClosed", err)
	}
	got := out.String()
	const prefix = `{"jsonrpc":"2.0","method":"chain","params":{"id":"`
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("client got %q, want the upstream's chain notification", got)
	}
	id, _, _ := strings.Cut(strings.TrimPrefix(got, prefix), `"`)
	return id
}

func TestRunCommandSetsChainEnv(t *testing.T) {
	t.Setenv(switchyard.ChainEnv, "from-env")
	if got := chainSeenByUpstream(t, switchyard.Options{ChainID: "from-options"}); got != "from-options" {
		t.Errorf("upstream saw chain %q, want %q", got, "from-options")
	}
	if got := chainSeenByUpstream(t, switchyard.Options{}); got != "from-env" {
		t.Errorf("upstream saw chain %q, want %q", got, "from-env")
	}
}
