# Changelog

Notable changes to `wcp-library`. Consumers pin exact versions, so anything
needing a change on the caller's side is listed under **Breaking**, with what to
do about it.

Releases before 1.15.0 predate this file; see the git history for those.

## 1.15.0

### Breaking

- **`graph.sharepoint.upload_multiple_files` now raises.** When any file in the
  batch fails it raises an `ExceptionGroup` holding one
  `requests.RequestException` per failed file, each carrying a note naming its
  file, instead of returning `{"filename": ..., "error": ...}` entries in the
  result list. The previous behaviour let a batch in which every upload failed
  look like one that had succeeded. If you inspected the result entries for
  errors, wrap the call in `try/except*` instead. Successful responses are
  discarded on failure; re-running the batch is the intended recovery and is
  safe with `conflict_behavior="replace"`. See
  `docs/adr/0003-upload-multiple-files-raises.md`.

### Added

- **`graph.GraphCredentials`** — an app registration's identity, from which
  tokens are minted on demand. Pass one anywhere a Graph helper takes `headers`
  and every request it makes re-mints and retries once on HTTP 401, which fixes
  processes that run longer than a token's ~1 hour lifetime. Built with
  `GraphCredentials.from_vault(api_key, credential_id)` or
  `GraphCredentials.from_app_registration(app_id, app_secret, tenant_id)`.
  Safe to share between threads: concurrent callers that met the same expired
  token produce one re-mint between them. `get_headers` and
  `get_headers_from_vault` are unchanged and remain right for short-lived
  callers.
- The `headers` parameter of every `graph.sharepoint`, `graph.mail`, and
  `graph.subscription` helper now also accepts a `GraphCredentials`.
- Documentation for `upload_multiple_files`, which was public but undocumented.

### Changed

- Minting a token is now retried on transient failure (a vault or token-endpoint
  blip), since a re-mint sits on the critical path of a long-running job rather
  than only at its start. It still raises `GraphAuthError` when it gives up.
  `get_headers` and `get_headers_from_vault` themselves are untouched, so no
  existing caller's timing changes.
- `graph.mail.get_email_metadata` return annotation narrowed from `dict | None`
  to `dict`; the body had no path that returned `None`. Runtime behaviour is
  unchanged.
- *Internal:* the tenacity-decorated Graph transport is now
  `wcp_library.graph._send`, and `wcp_library.graph._request` is the wrapper
  that adds token-expiry handling. Both are private, and helpers still call
  `_request`, but tests elsewhere that patch `wcp_library.graph._request` to
  simulate transport behaviour may want `_send` instead.

### Fixed

- Wiki: `get_lists` was documented as returning `[]` on error. It has raised
  since 1.13; the page had not caught up.
- Wiki and docstrings: the usage examples imported `get_auth_headers`, which
  does not exist. They now import `get_headers`.
- `graph.mail.get_email_metadata` documented a `notification` parameter it does
  not take, and omitted its `mailbox`, `message_id`, and `:raises:` entries.
- `graph.mail.get_mailbox_folders` did not document `mailbox` or
  `parent_folder_id`, and `graph.mail.save_attachment` wrote its parameter
  types inside the parameter names (`:param source (dict | bytes):`) and did
  not document the `TypeError` it raises.
- `graph.subscription` module docstring carried a note asking for its error
  contract to be reconciled with `_GraphRetriable`'s docstring, which had
  already been done.
