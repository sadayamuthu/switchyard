# Security Policy

## Reporting a vulnerability

Please don't report security problems in a public issue, pull request or
discussion. Use
[**Report a vulnerability**](https://github.com/sadayamuthu/switchyard/security/advisories/new)
on the repository's Security tab instead. Only the maintainers can see the
report.

Include what you can of:

- the switchyard version (`switchyard version`) or commit, your Go version and OS
- how switchyard was used, and its configuration with secrets removed
- steps to reproduce
- what happened and what you expected

You'll get a reply when the report has been read, and again when a fix is
ready. A fix ships as a patch release; the advisory names you unless you'd
rather it didn't.

## Supported versions

switchyard is pre-1.0. Fixes land in the latest release only; please upgrade
before reporting.

## What counts

switchyard runs an upstream MCP server as a child process and forwards messages between it and the client, passing tool calls through any hooks you register.

switchyard is a security tool, so anything that breaks one of these promises
is a security issue:

- **Transparent by default.** With no hooks, every message passes through unchanged. A message that is altered, dropped, reordered against the rules in the docs, or injected is in scope.
- **Hooks see only what they're given.** A way for one session's state to leak into another session is in scope.
- **Traces go only where you send them.** switchyard writes events only to the sink you configure, and makes no network calls of its own.

These are by design, and not vulnerabilities:

- Hooks you register can do anything to traffic. Review them like any code in your agent's path.
- Traces contain tool arguments and results verbatim.

A wrong result that isn't covered above is a bug, not a vulnerability. Report
it in a normal issue; see [CONTRIBUTING.md](CONTRIBUTING.md).
