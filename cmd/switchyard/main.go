// Command switchyard is a pass-through MCP stdio proxy for debugging: it runs
// an upstream server command, forwards every message unchanged and, with
// --trace, writes a JSONL event for every request and response.
//
// Usage:
//
//	switchyard [--name NAME] [--trace FILE] -- <cmd> [args...]
//
// It writes nothing to stdout but protocol messages; diagnostics go to
// stderr. It exits 0 when the client's input ends cleanly, 1 on an error and
// 2 on bad usage.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sadayamuthu/switchyard"
)

const usage = "usage: switchyard [--name NAME] [--trace FILE] -- <cmd> [args...]"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// traceHook is a no-op hook: events are emitted only for hooked requests, so
// installing it makes the trace record every request.
type traceHook struct{ switchyard.Base }

func (traceHook) Name() string { return "trace" }

func run(args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("switchyard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, usage)
		fs.PrintDefaults()
	}
	name := fs.String("name", "switchyard", "name of this hop in trace events")
	trace := fs.String("trace", "", "append JSONL trace events to `FILE`")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	argv := fs.Args()
	if len(argv) == 0 {
		fmt.Fprintln(stderr, "switchyard: no upstream command")
		fs.Usage()
		return 2
	}

	opts := switchyard.Options{Name: *name, ChainID: switchyard.ChainIDFromEnv()}
	if *trace != "" {
		f, err := os.OpenFile(*trace, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintln(stderr, "switchyard:", err)
			return 1
		}
		defer f.Close()
		opts.Hooks = []switchyard.Hook{traceHook{}}
		opts.Sink = switchyard.NewJSONLSink(f)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := switchyard.RunCommand(ctx, switchyard.New(opts), argv, stdin, stdout); err != nil {
		fmt.Fprintf(stderr, "switchyard %s: %v\n", *name, err)
		return 1
	}
	return 0
}
