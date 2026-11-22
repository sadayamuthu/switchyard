# Roadmap

What switchyard plans to work on next. It's a plan, not a promise: priorities
change with what users report. Discuss any item in an issue before starting
on it. The reasoning behind the project, and what would make us stop, is in
[THESIS.md](THESIS.md).

The v0 implementation plan is in
[docs/superpowers/plans/](docs/superpowers/plans/); the design is shared
with AgentChaos.

## Now: v0.1

- JSON-RPC pass-through proxy over stdio, with the transparency test suite
- Hook interface for `tools/call`; sessions; JSONL traces

## Next: v0.2 to v0.x

- `testkit`
- AgentChaos v0 as the first consumer

## Later: 1.0

- Streamable HTTP transport; approval holds (for Rewind and Fuse)
- **Stable interfaces.** Freeze the public formats and APIs, document a
  versioning policy, and only break them in a major release.

## Community goals

- Maintainers from at least two organizations (see
  [GOVERNANCE.md](GOVERNANCE.md)).
- Documented production or research use by at least two unaffiliated
  organizations.
