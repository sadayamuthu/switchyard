package switchyard_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/sadayamuthu/switchyard"
	"github.com/sadayamuthu/switchyard/testkit"
)

// Paths of the binaries TestMain builds.
var switchyardBin, testserverBin string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "switchyard-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: creating temp dir:", err)
		return 1
	}
	defer os.RemoveAll(dir)
	switchyardBin = filepath.Join(dir, "switchyard")
	testserverBin = filepath.Join(dir, "testserver")
	for bin, pkg := range map[string]string{switchyardBin: "./cmd/switchyard", testserverBin: "./internal/testserver"} {
		if out, err := exec.Command("go", "build", "-o", bin, pkg).CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: building %s: %v\n%s", pkg, err, out)
			return 1
		}
	}
	return m.Run()
}

// envWithout returns the process environment without key.
func envWithout(key string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, key+"=") {
			env = append(env, kv)
		}
	}
	return env
}

// chain describes one run of client → switchyard a → switchyard b → testserver.
type chain struct {
	cs             *mcp.ClientSession
	cmd            *exec.Cmd
	traceA, traceB string
}

// startChain starts the two-proxy chain with environment env and connects an
// SDK client to it.
func startChain(t *testing.T, env []string) *chain {
	t.Helper()
	dir := t.TempDir()
	c := &chain{traceA: filepath.Join(dir, "a.jsonl"), traceB: filepath.Join(dir, "b.jsonl")}
	c.cmd = exec.Command(switchyardBin, "--name", "a", "--trace", c.traceA, "--",
		switchyardBin, "--name", "b", "--trace", c.traceB, "--",
		testserverBin)
	c.cmd.Env = env
	c.cmd.Stderr = os.Stderr
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "v0.0.0"}, nil).
		Connect(ctx, &mcp.CommandTransport{Command: c.cmd}, nil)
	if err != nil {
		t.Fatalf("connecting through the chain: %v", err)
	}
	c.cs = cs
	return c
}

// close shuts the client down and checks that the chain exited cleanly.
func (c *chain) close(t *testing.T) {
	t.Helper()
	if err := c.cs.Close(); err != nil {
		t.Errorf("closing the client: %v", err)
	}
	if code := c.cmd.ProcessState.ExitCode(); code != 0 {
		t.Errorf("switchyard a exited with code %d, want 0", code)
	}
}

func callText(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("calling %s: %v", tool, err)
	}
	if res.IsError {
		t.Fatalf("calling %s: tool error %+v", tool, res.Content)
	}
	return resultText(t, res)
}

// readTrace decodes a JSONL trace file.
func readTrace(t *testing.T, path string) []switchyard.Event {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading trace: %v", err)
	}
	var events []switchyard.Event
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, len(data)+1) // a line may be as long as the file
	for sc.Scan() {
		var e switchyard.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("trace %s: bad line %q: %v", path, sc.Text(), err)
		}
		events = append(events, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("reading trace %s: %v", path, err)
	}
	if len(events) == 0 {
		t.Fatalf("trace %s is empty", path)
	}
	return events
}

// traceChain checks that every event in the trace at path is from hop and
// carries one chain ID, and that the trace holds the echo request; it
// returns the chain ID.
func traceChain(t *testing.T, path, hop string) string {
	t.Helper()
	events := readTrace(t, path)
	chainID := events[0].Chain
	sawEcho := false
	for _, e := range events {
		if e.Chain != chainID {
			t.Errorf("trace %s: chain IDs %q and %q", path, chainID, e.Chain)
		}
		if e.Hop != hop {
			t.Errorf("trace %s: hop %q, want %q", path, e.Hop, hop)
		}
		if e.Kind == switchyard.KindRequest && e.Method == "tools/call" && e.Tool == "echo" {
			sawEcho = true
			if e.Decision != "continue" {
				t.Errorf("trace %s: echo decision %q, want continue", path, e.Decision)
			}
		}
	}
	if !sawEcho {
		t.Errorf("trace %s has no echo request: %+v", path, events)
	}
	return chainID
}

func TestChainOfTwoProxies(t *testing.T) {
	const chainID = "e2e-chain-0001"
	c := startChain(t, append(envWithout(switchyard.ChainEnv), switchyard.ChainEnv+"="+chainID))
	if got := callText(t, c.cs, "echo", map[string]any{"text": "through two hops"}); got != "through two hops" {
		t.Errorf("echo = %q, want %q", got, "through two hops")
	}
	if got := callText(t, c.cs, "env", nil); got != chainID {
		t.Errorf("testserver saw chain %q, want %q", got, chainID)
	}
	c.close(t)
	if got := traceChain(t, c.traceA, "a"); got != chainID {
		t.Errorf("trace a chain = %q, want %q", got, chainID)
	}
	if got := traceChain(t, c.traceB, "b"); got != chainID {
		t.Errorf("trace b chain = %q, want %q", got, chainID)
	}
}

func TestChainIDGeneratedOnce(t *testing.T) {
	c := startChain(t, envWithout(switchyard.ChainEnv))
	callText(t, c.cs, "echo", map[string]any{"text": "x"})
	seen := callText(t, c.cs, "env", nil)
	c.close(t)
	a, b := traceChain(t, c.traceA, "a"), traceChain(t, c.traceB, "b")
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(a) {
		t.Errorf("generated chain ID %q is not 16 hex characters", a)
	}
	if a != b || a != seen {
		t.Errorf("chain IDs differ: trace a %q, trace b %q, testserver %q", a, b, seen)
	}
}

func TestSwitchyardBinaryUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"--"}, {"--bogus", "--", "true"}} {
		cmd := exec.Command(switchyardBin, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 2 {
			t.Errorf("switchyard %q: err = %v, want exit code 2", args, err)
		}
		if stdout.Len() != 0 {
			t.Errorf("switchyard %q wrote to stdout: %q", args, stdout.String())
		}
		if !strings.Contains(stderr.String(), "usage") {
			t.Errorf("switchyard %q: stderr %q has no usage", args, stderr.String())
		}
	}
}

func TestSwitchyardBinaryUpstreamMissing(t *testing.T) {
	const bin = "switchyard-no-such-binary-e2e"
	cmd := exec.Command(switchyardBin, "--", bin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Errorf("err = %v, want exit code 1", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("wrote to stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), bin) {
		t.Errorf("stderr %q does not name %q", stderr.String(), bin)
	}
}

// BenchmarkHop measures an echo round trip with no proxy and through one
// in-process proxy; the difference is the per-hop overhead.
func BenchmarkHop(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		benchEcho(b, testkit.ConnectDirect(b, testkit.Server(b, echoTool())))
	})
	b.Run("proxy", func(b *testing.B) {
		benchEcho(b, testkit.Connect(b, testkit.Server(b, echoTool()), switchyard.Options{Name: "bench"}))
	})
}

func benchEcho(b *testing.B, cs *mcp.ClientSession) {
	ctx := context.Background()
	params := echoCall("bench")
	for b.Loop() {
		if _, err := cs.CallTool(ctx, params); err != nil {
			b.Fatal(err)
		}
	}
}

// The binary exits once its upstream does, even though its stdin, a
// blocking-mode pipe whose writer stays open, never reaches EOF.
func TestSwitchyardBinaryExitsWhenUpstreamExits(t *testing.T) {
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinW.Close()
	cmd := exec.Command(switchyardBin, "--", "sh", "-c", "exit 0")
	cmd.Stdin = stdinR
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = stdinR.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 1 {
			t.Errorf("err = %v, want exit code 1", err)
		}
		if !strings.Contains(stderr.String(), "upstream closed") {
			t.Errorf("stderr %q does not report the upstream closing", stderr.String())
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("switchyard did not exit within 3s of its upstream exiting")
	}
}

// TestReadTraceLongLine checks that readTrace reads an event line longer
// than bufio.Scanner's default 64KB limit in full.
func TestReadTraceLongLine(t *testing.T) {
	big := `"` + strings.Repeat("x", 100_000) + `"`
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	var buf bytes.Buffer
	sink := switchyard.NewJSONLSink(&buf)
	sink.Emit(switchyard.Event{V: 1, Kind: switchyard.KindRequest, Method: "a", Params: json.RawMessage(big)})
	sink.Emit(switchyard.Event{V: 1, Kind: switchyard.KindResponse, Method: "b"})
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	events := readTrace(t, path)
	if len(events) != 2 || string(events[0].Params) != big || events[1].Method != "b" {
		t.Errorf("readTrace got %d events, want both with the long params intact", len(events))
	}
}
