# switchyard: thesis

**In one line:** one small, transparent, well-tested MCP interception core
is enough for every runtime security and resilience tool to be a thin plug-in
instead of its own proxy.

## The problem

Fault injection, intent enforcement, taint tracking, circuit breaking, undo
logging and sandboxing all need the same position: on the path between an
agent and its tools, seeing each call before and after it runs. Building that
position correctly (framing, ID mapping, cancellation, pass-through of unknown
features) is most of the work, and it's the same work every time.

## The bet

1. **One hook shape covers all six tools.** Continue, rewrite, block,
   synthesize and hang on requests, plus observe and rewrite on responses,
   with per-session state, is enough for AgentChaos, IntentLock, Taintline,
   Fuse, Rewind and Airlock's proxy mode.
2. **Transparency makes chaining work.** If each hop is invisible to the
   next, independent tools can be stacked in any order without knowing about
   each other.
3. **Separate binaries beat one plug-in host.** Independent repos and
   releases are worth a small cost per hop.
4. **Building it under AgentChaos hardens it.** A tool whose job is to break
   calls in every way is the best test of the layer that carries them.

## Why now

MCP has become the common tool interface across agent clients, so one
interception layer reaches most agents, and OpenAstra has six tools waiting
on it.

## What success looks like

- AgentChaos ships on switchyard, then at least two more tools (Fuse and
  IntentLock first) ship without changing the hook interface in a breaking
  way.
- The transparency test suite passes against real MCP clients and servers.
- A chain of four hops adds no delay a person notices.

## What would prove us wrong

- Tools need incompatible hook shapes, so the interface grows tool-specific
  special cases.
- MCP spec changes force frequent breaking changes in the core.
- Per-hop overhead makes chaining impractical, forcing a combined binary.
- Agents increasingly call tools in ways a proxy can't see (built-in tools,
  in-process frameworks).

## Not a bet on

Being a general MCP gateway product (auth, routing, discovery), or any
single tool's logic.

## Where it fits

The shared foundation for OpenAstra areas VIII and X. First consumer:
[AgentChaos](https://github.com/sadayamuthu/agentchaos).
