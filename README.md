# switchyard

The shared MCP proxy core for OpenAstra's runtime tools. `switchyard` is a Go
library that sits between an AI agent and its MCP tool servers, passes every
message through untouched, and lets a tool plug in code that runs on each
call: observe it, delay it, block it, answer it, or rewrite the result.

> **Status: design stage.** There is no code yet. The interface below is the
> proposal, and it will change. Part of the [OpenAstra](https://openastra.org)
> research agenda; the shared foundation for areas VIII (AI Runtime Security)
> and X (AI Resilience & Recovery).

## The problem

Six OpenAstra tools need to sit on the agent's tool path and act on each
call. Without a shared core, each would re-implement the same hard parts:
framing MCP messages, matching requests to responses, handling cancellation,
and passing through protocol features it doesn't understand. Six copies means
six sets of bugs, and six proxies that may interfere with each other when
stacked.

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
4. **Traces.** Every call, decision and result is written as versioned JSONL
   events. Hops in a chain share an ID so their traces can be joined.

## Stacking tools

Each tool is its own binary built on switchyard. Tools stack by chaining:

```
agent → fuse → intentlock → taintline → rewind → MCP server
```

The order of the chain is the order of the checks.

## Built on switchyard

| Tool | What it does with each call |
|------|-----------------------------|
| [AgentChaos](https://github.com/sadayamuthu/agentchaos) | Injects faults: timeouts, errors, partial and malformed results |
| [IntentLock](https://github.com/sadayamuthu/intentlock) | Blocks calls outside the user's capability envelope |
| [Taintline](https://github.com/sadayamuthu/taintline) | Blocks calls whose arguments carry untrusted data |
| [Fuse](https://github.com/sadayamuthu/fuse) | Counts calls and trips a breaker on runaway behaviour |
| [Rewind](https://github.com/sadayamuthu/rewind) | Records calls and the data needed to undo them |
| [Airlock](https://github.com/sadayamuthu/airlock) | Routes risky calls into a per-call sandbox (MCP proxy mode) |

## Planned surfaces

- Go library: proxy, hook interface, sessions, event sink
- `testkit` package: scriptable fake MCP server and client, used by every
  consumer's tests
- `switchyard` binary: pass-through proxy that only writes the trace, for
  debugging chains
- stdio transport first; Streamable HTTP next

## Design principles

- **No tool-specific logic in the core.** If it only makes sense for one
  tool, it lives in that tool.
- **Transparent by default.** Unknown methods and capabilities pass through.
- **No model in the loop.**
- **Cheap per hop**, so stacking four tools isn't noticeable.

## Roadmap

1. JSON-RPC pass-through proxy over stdio, with the transparency test suite
2. Hook interface for `tools/call`; sessions; JSONL traces
3. `testkit`
4. AgentChaos v0 as the first consumer
5. Streamable HTTP transport; approval holds (for Rewind and Fuse)

## License

To be decided before the first release.
