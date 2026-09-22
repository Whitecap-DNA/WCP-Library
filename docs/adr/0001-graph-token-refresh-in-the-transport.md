# Token refresh lives in the Graph transport, not around the caller's operation

Graph access tokens expire after roughly an hour, so a caller holding one
headers dict for a long run sees every call fail once it does. The obvious fix
is a session object that wraps each operation (`session.call(lambda h: ...)`),
which is what the originating design proposed. We put the re-mint inside
`wcp_library.graph._request` instead, so every one of the ~36 public Graph
helpers gains expiry handling without being changed or wrapped.

## Considered Options

- **A session wrapping the operation.** Explicit and simple, but opt-in: anyone
  who adds a Graph call and passes `credentials.headers` directly reintroduces
  the bug, and neither a type checker nor a linter catches it. Thin per-helper
  methods would close that hole at the cost of ~36 near-identical methods.
- **Refresh inside the transport (chosen).** One function changes; no call site
  does. It also covers `upload_multiple_files`, which runs a thread per file
  through `_request` and would otherwise need handling of its own.

## Consequences

The unit retried is one HTTP request rather than one logical operation, which is
strictly better for idempotency: a paginating helper resumes at the page it was
on, and a multi-file upload does not repeat the files that already landed.

`_request` is no longer a dumb transport — it knows about the `GraphCredentials`
type. The tenacity-decorated function was renamed `_send` and `_request` became
the wrapper, specifically so the ~36 helpers and the 86 test patch points naming
`_request` keep working untouched. Reversing this means renaming those back, not
rewriting call sites.

`GraphCredentials.call` is retained as a documented escape hatch for Graph
endpoints this library does not wrap.
