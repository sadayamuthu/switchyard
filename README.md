<div align="center">

# switchyard

<h3>The shared MCP proxy core for OpenAstra runtime tools</h3>

[![Status](https://img.shields.io/badge/status-v0.1.0-yellow?style=flat-square)](ROADMAP.md)
[![Go Reference](https://pkg.go.dev/badge/github.com/sadayamuthu/switchyard.svg)](https://pkg.go.dev/github.com/sadayamuthu/switchyard)
[![Go 1.26+](https://img.shields.io/badge/go-1.26+-00ADD8?style=flat-square)](https://go.dev/dl/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](LICENSE)

---

[How it works](#how-it-works) · [Usage](#usage) · [Thesis](THESIS.md) · [Roadmap](ROADMAP.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

</div>

---

## What is switchyard?

The shared MCP proxy core for OpenAstra's runtime tools. `switchyard` is a Go
library that sits between an AI agent and its MCP tool servers, passes every
message through untouched, and lets a tool plug in code that runs on each
call: observe it, delay it, block it, answer it, or rewrite the result.

> **Status: v0.1.0, early release.** The library, testkit and debug binary
> work and are tested, but the API may still change before 1.0. Part of the
> [OpenAstra](https://openastra.org) research agenda; the shared foundation for
> areas VIII (AI Runtime Security) and X (AI Resilience & Recovery).

---

## The problem

Six OpenAstra tools need to sit on the agent's tool path and act on each
call. Without a shared core, each would re-implement the same hard parts:
framing MCP messages, matching requests to responses, handling cancellation,
and passing through protocol features it doesn't understand. Six copies means
six sets of bugs, and six proxies that may interfere with each other when
stacked.

---

## How it works

```
agent  ──►  switchyard proxy  ──►  MCP tool server
                 │
              hooks (your tool's logic)
```

1. **Transparent by default.** With no hooks, a client can't tell switchyard
   is there: same capabilities, tools, notifications and errors.
2. **Hooks by MCP method.** A hook sees a request before it goes upstream and
   the response on the way back. For `tools/call` it can continue, rewrite
   arguments, block, answer with a synthesized result, or hang.
3. **Sessions.** Hooks get per-connection state that lasts across calls
   (counters, envelopes, taint labels).
4. **Traces.** Hooked requests, their decisions and their responses are
   written as versioned JSONL events. Events are emitted only when at least
   one hook is installed; the `switchyard` binary installs a no-op hook under
   `--trace` so that every request is recorded. Hops in a chain share an ID so
   their traces can be joined.

---

## Stacking tools

Each tool is its own binary built on switchyard. Tools stack by chaining:

```
agent → agentfuse → agentlock → agenttaint → agentrewind → MCP server
```

The order of the chain is the order of the checks.

---

## Built on switchyard

| Tool | What it does with each call |
|------|-----------------------------|
| [AgentChaos](https://github.com/sadayamuthu/agentchaos) | Injects faults: timeouts, errors, partial and malformed results |
| [AgentLock](https://github.com/sadayamuthu/agentlock) | Blocks calls outside the user's capability envelope |
| [AgentTaint](https://github.com/sadayamuthu/agenttaint) | Blocks calls whose arguments carry untrusted data |
| [AgentFuse](https://github.com/sadayamuthu/agentfuse) | Counts calls and trips a breaker on runaway behaviour |
| [AgentRewind](https://github.com/sadayamuthu/agentrewind) | Records calls and the data needed to undo them |
| [AgentBox](https://github.com/sadayamuthu/agentbox) | Routes risky calls into a per-call sandbox (MCP proxy mode) |

---

## Usage

Add the library to your module:

```sh
go get github.com/sadayamuthu/switchyard@latest
```

Or install the debug binary:

```sh
go install github.com/sadayamuthu/switchyard/cmd/switchyard@latest
```

A tool is a hook plus a few lines to run the real server behind the proxy:

```go
// denyRm blocks calls to the rm tool and lets everything else through.
type denyRm struct{ switchyard.Base }

func (denyRm) Name() string { return "deny-rm" }

func (denyRm) OnRequest(ctx context.Context, s *switchyard.Session, r *switchyard.Request) switchyard.Decision {
	if tc, ok := r.ToolCall(); ok && tc.Name == "rm" {
		return switchyard.BlockTool("rm is not allowed here")
	}
	return switchyard.Continue()
}

func main() {
	p := switchyard.New(switchyard.Options{
		Name:  "deny-rm",
		Hooks: []switchyard.Hook{denyRm{}},
		Sink:  switchyard.NewJSONLSink(os.Stderr), // one JSONL event per request and response
	})
	// Run the real server (os.Args[1:]) and proxy this process's stdio to it.
	if err := switchyard.RunCommand(context.Background(), p, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The `switchyard` debug binary is a pass-through proxy that can write a trace:

```sh
go install github.com/sadayamuthu/switchyard/cmd/switchyard@latest
switchyard [--name NAME] [--trace FILE] -- <cmd> [args...]
```

---

## What's included

- Go library: proxy, hook interface, sessions, versioned JSONL events, chain
  IDs, and `RunCommand` to run an upstream server command
- `testkit` package: wires a real MCP SDK client and server through a proxy,
  for switchyard's own tests and for every consumer's
- `switchyard` binary: pass-through proxy that only writes the trace, for
  debugging chains
- stdio transport

---

## Planned surfaces

- Streamable HTTP transport

---

## Design principles

- **No tool-specific logic in the core.** If it only makes sense for one
  tool, it lives in that tool.
- **Transparent by default.** Unknown methods and capabilities pass through.
- **No model in the loop.**
- **Cheap per hop**, so stacking four tools isn't noticeable.

---

## Roadmap

The plan, from the first working version to 1.0, is in [ROADMAP.md](ROADMAP.md). The reasoning behind the project, and what would make us stop, is in [THESIS.md](THESIS.md).

---

## Contributing

Design feedback, real-world cases and fixes are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to help while v0 is in development, and [GOVERNANCE.md](GOVERNANCE.md) for how decisions are made.

---

## Security

Please report vulnerabilities privately through [GitHub security advisories](https://github.com/sadayamuthu/switchyard/security/advisories/new), not in a public issue. See [SECURITY.md](SECURITY.md) for what to include and what's in scope.

---

## License

[Apache License 2.0](LICENSE)

---

<div align="center">

**switchyard: the shared MCP proxy core for OpenAstra runtime tools**

switchyard is managed by [OpenAstra](https://openastra.org)

[GitHub](https://github.com/sadayamuthu/switchyard) · [Thesis](THESIS.md) · [Roadmap](ROADMAP.md)

</div>
