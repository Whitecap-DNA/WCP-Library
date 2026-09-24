# Changelog

Notable changes to `wcp-library`. Consumers pin exact versions, so anything
needing a change on the caller's side is listed under **Breaking**, with what to
do about it.

Releases before 1.15.0 predate this file; see the git history for those.

## 1.15.3

### Breaking

- **`browser_automation` `sharepoint_config` now takes a `GraphCredentials`.**
  The `credentials` key replaces `app_id`, `app_secret`, and `tenant_id`;
  `site_id` is unchanged. This keeps the client secret out of a dict handed to
  a browser object, reuses one token across screenshots instead of requesting a
  fresh one per screenshot, and lets `from_vault` pick up a rotated secret.

  > Migration:
  > ```python
  > sp_config = {
  >     "credentials": GraphCredentials.from_vault(vault_api_key, credential_id),
  >     "site_id": site_id,
  > }
  > ```
  > Use `GraphCredentials.from_app_registration(app_id, app_secret, tenant_id)`
  > to keep passing those three values directly.

  Note this is a breaking change in a patch release, contrary to the policy at
  the top of this file. It was a deliberate call to keep it in this release
  rather than hold it for 1.16.0.

### Changed

- **`graph.sharepoint.upload_multiple_files` raises again.** 1.15.1 reverted it
  to returning `{"filename": ..., "error": ...}` entries; it now raises an
  `ExceptionGroup` of the failures as 1.15.0 did, per
  `docs/adr/0003-upload-multiple-files-raises.md`. It also collects **every**
  exception type rather than only `requests.RequestException` and `TypeError`:
  an exception left uncaught in a worker thread never reached the caller at
  all, it printed a traceback and left that file's entry empty, which is the
  silent failure the contract exists to prevent.

  > Migration: `try` / `except*` instead of inspecting result entries.

### Fixed

- **`graph.subscription.recreate_subscription` pointed the new subscription at
  a dead endpoint.** `create_subscription` appends `/api/graph` and
  `/api/lifecycle` to the URL it is given, but `recreate_subscription` passed
  the *stored* `notificationUrl` straight back in, so a recreated subscription
  was registered against `<base>/api/graph/api/graph`. Graph accepts the
  subscription, and notifications then go nowhere. The base URL is now
  recovered before recreating.
- **`recreate_subscription` forwarded missing fields as `None`.** A
  subscription that came back without `notificationUrl`, `resource`,
  `changeType`, or `clientState` produced a malformed payload, or crashed
  inside `get_resource_type` with an `AttributeError`. It now raises
  `ValueError` naming the missing field.
- `credentials` (synchronous): the `requests.HTTPError` handlers formatted
  `e.response.status_code` without checking that `.response` is set, so an
  `HTTPError` carrying no response replaced the real failure with an
  `AttributeError` from inside the error handler.
- Type annotations that promised less than the code accepts:
  `graph._iter_pages` rejected the `GraphCredentials` every caller passes it,
  `REQUEST_TIMEOUT` was typed `int` though `set_request_timeout` accepts a
  float, and `get_credential_from_id` was typed `int` though
  `get_headers_from_vault` passes `int | str`. `get_headers_from_vault` also
  documented a coercion to `int` that it never performed; the claim is gone.

## 1.15.2

### Fixed

- `graph.subscription.get_resource_type` misclassified four resource shapes
  that nest a specific collection under a generic scope segment: Teams channel
  messages and chat messages resolved to `mail`, and To Do tasks and
  user-scoped Copilot resources resolved to `directory`. Each was given the
  lifetime and notification parser of the wrong resource type. Classification
  is now an ordered, most-specific-first match over the path segments.

## 1.15.1

### Breaking

- **`graph.sharepoint.upload_multiple_files` stopped raising** and returned
  `{"filename": ..., "error": ...}` entries again, reverting the 1.15.0
  contract. Restored in 1.15.3 — if you are pinned to 1.15.1 or 1.15.2, a
  failed upload in a batch is reported in the result list and is easy to miss.
- **`graph.sharepoint.get_file_metadata` was renamed `get_item_metadata`**, and
  gained `item_id` addressing. The old name is gone, so a call to it raises
  `AttributeError`.
- **`graph.mail.parse_email_notification` was removed.** Use
  `graph.get_resource_context`, which covers mail alongside every other
  subscribable resource type and returns a dict rather than a two-tuple:
  `context["user_id"]` and `context["message_id"]` replace the old tuple.
- Several file helpers gained keyword-only `item_id`, and `move_file` and
  `copy_file` gained `destination_id`, to address items by Graph ID instead of
  path; `download_file` gained `filename`. These are additive, but `site_id`
  and the path arguments became optional (`str | None`) to support the new
  mode, so a positional call that relied on their order is worth re-checking.

### Added

- `graph.get_resource_context` — parses an incoming change or lifecycle
  notification into the identifiers needed to act on the resource behind it.
- `graph.subscription.get_resource_type` is now public.
- `graph.sharepoint.get_changed_items` and `get_item_metadata` — delta (change)
  tracking for drive items.
- Folder addressing by `item_id` on the upload helpers, as an alternative to a
  path, and `site_id` became optional where `drive_id` is given directly.

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

- **`wcp_library.informatica` removed.** Informatica is no longer used as a
  product at WCAP, so the module and its `InformaticaSession` client are gone,
  with no replacement in this library. The last release containing it is
  1.14.11. Its wiki page is kept as a removal notice rather than deleted, since
  the wiki sync never deletes pages and an old link should resolve to an
  explanation.
- **Credential writes raise instead of returning `False`.**
  `update_credential` and `new_credentials`, on every credential manager, sync
  and async, now raise the new `CredentialWriteError` when the Vault rejects a
  write or the request fails. They previously returned `False`, so a caller who
  did not inspect the return value carried on as though the write had
  succeeded. Both still return `True` on success, so code that checks for a
  truthy result keeps working; code shaped like
  `if not manager.update_credential(d): ...` should become a
  `try` / `except CredentialWriteError`.

### Added

- **`credentials.CredentialWriteError`** — raised when a credential cannot be
  written to the Vault. The counterpart to `MissingCredentialsError`, which
  means a credential could not be read.
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

- `credentials` (synchronous): `update_credential` raised a bare
  `IndexError` when no vault entry matched the username, because it indexed
  the first element of an unchecked filter result. It now raises
  `MissingCredentialsError`, matching the asynchronous manager, which
  already guarded this.
- `credentials` (synchronous): the `PUT` in `update_credential` had no
  timeout, so a stalled vault could block a caller indefinitely. It now
  uses the same 30 second timeout as every other call in the module.
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
