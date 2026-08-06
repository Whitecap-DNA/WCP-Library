# Spec: replace `email_reporting` with a generic `email_report`

**Status:** Approved — ready for implementation plan
**Date:** 2026-08-06
**Author:** Mitch Petersen (with Claude)
**Scope:** `wcp_library/emailing.py`; `tests/emailing/test_emailing.py`; `docs/Wiki Docs/Helper ‐ Emailing`

## Motivation

`MailServer.email_reporting(subject, body)` is a thin, hardcoded convenience: plain-text body, fixed sender (`python@wcap.ca`), fixed recipient (`Reporting@wcap.ca`). That address is being phased out.

Separately, downstream projects have been independently building rich HTML "something failed" report emails on top of `MailServer.send_email` — a reference implementation was copied into this repo's root as `email.py` (not part of this package; it depends on `structlog`, which isn't a `wcp_library` dependency, confirming it's an external consumer's file, kept here only for reference). Its `_render_endpoint_failed` renderer, and the shared layout it builds on (`_render_layout`, `_facts_table`, `_pre_block`, `_callout`, `_esc`), produce a banner + facts-table + error/traceback HTML email — mostly generic, with a few pieces specific to that project's domain (Postgres column errors, "endpoint" terminology, event-based dispatch).

This spec ports the generic parts of that pattern into `wcp_library.emailing` as a new `MailServer.email_report(...)` method, replacing `email_reporting` outright, so any script using this library gets a reusable, tested way to send a well-formatted error-report email instead of reinventing HTML templating per project.

## Goals

- `MailServer.email_report(...)` replaces `MailServer.email_reporting(...)`. Sender and recipients become explicit parameters (matching `send_email`) — no hardcoded address.
- `project: str` identifies the calling script/system (e.g. `"api-ingest"`, `"invoice-scraper"`). Used to prefix the email subject and to label the report in the body and footer, so a reader of a shared inbox can tell which system sent it.
- `facts: list[tuple[str, Any]] | None` — a caller-defined label/value table. No fixed keys; the method has no opinion on content. Rows whose value is `None`, `""`, or `[]` are skipped. Omitted entirely (no table rendered) when `facts` is `None` or empty after filtering.
- `error: BaseException | None` — when given, the method derives both a one-line summary and the full traceback directly from the exception object (`type(error).__name__: str(error)`, and `traceback.format_exception(error)`), so the caller passes the exception exactly as caught (`except Exception as e: ... email_report(..., error=e)`) rather than pre-formatting two separate strings.
- `sender`, `recipients`, `cc`, `bcc`, `attachments` are forwarded to `send_email` unchanged — same types, same validation (approved-sender check, address-format check), same attachment handling.
- The email is always styled the same way: one fixed banner color, no severity levels, no callout/highlight boxes. This method exists exclusively to report errors — there is nothing dynamic to select between.

## Non-goals

- No backward-compatibility shim for `email_reporting` — it is deleted outright. This is a breaking change; the Reporting DL address it hardcoded is being retired anyway, so there is no migration path that keeps both working.
- No severity levels (error/warning/info) and no severity→color mapping. Considered and explicitly dropped.
- No callout/highlight box concept. Considered and explicitly dropped.
- No multi-traceback deduplication-with-counts feature. Considered and explicitly dropped — single `error` object in, one summary + one traceback out.
- No changes to `send_email`, `_build_message`, `_send`, `_normalise_addresses`, or `_build_attachment_part`.
- `./email.py` (the reference file at the repo root) is not part of the `wcp_library` package, is not imported by anything in this repo, and is not modified or deleted by this work — it exists only as a reference for this design.

## Architecture

### New private module-level helpers in `wcp_library/emailing.py`

Mirrors the existing private-helper pattern already in the file (`_normalise_addresses`, `_build_attachment_part` are module-level; `_build_message`, `_send` are private methods). None of the new helpers are exposed publicly.

```python
def _esc(value: Any) -> str:
    """
    Escape a value for safe inclusion in HTML.

    :param value: Any value — coerced to string before escaping.
    :return: HTML-safe string.
    """
    return html.escape(str(value), quote=True)


def _facts_table_html(rows: list[tuple[str, Any]]) -> str:
    """
    Render a label/value table, skipping rows with an empty value.

    :param rows: Ordered ``(label, value)`` pairs.
    :return: HTML ``<table>`` markup, or ``""`` if no row survives filtering.
    """
    cells: list[str] = []
    for label, value in rows:
        if value in (None, "", []):
            continue
        cells.append(
            "<tr>"
            f"<td style='padding: 4px 12px 4px 0; color: #6b7280; "
            f"vertical-align: top; white-space: nowrap;'>{_esc(label)}</td>"
            f"<td style='padding: 4px 0; font-family: monospace;'>{_esc(value)}</td>"
            "</tr>"
        )
    if not cells:
        return ""
    return (
        "<table style='border-collapse: collapse; margin: 8px 0;'>"
        + "".join(cells)
        + "</table>"
    )


def _pre_block_html(content: str) -> str:
    """
    Render text in a scrollable monospace block.

    :param content: Text to display verbatim (HTML-escaped).
    :return: HTML ``<pre>`` markup.
    """
    return (
        "<pre style='background: #f3f4f6; border: 1px solid #e5e7eb; "
        "border-radius: 4px; padding: 12px; font-size: 12px; "
        "line-height: 1.45; white-space: pre-wrap; word-break: break-word; "
        f"max-height: 400px; overflow: auto; margin: 8px 0;'>{_esc(content)}</pre>"
    )


def _format_exception(error: BaseException) -> tuple[str, str]:
    """
    Derive a one-line summary and the full traceback text from an exception.

    :param error: The caught exception.
    :return: ``(summary, traceback_text)``.
    """
    summary = f"{type(error).__name__}: {error}"
    traceback_text = "".join(traceback.format_exception(error))
    return summary, traceback_text


def _render_report_html(
    project: str, subject: str, facts: list[tuple[str, Any]] | None,
    error: BaseException | None,
) -> str:
    """
    Compose the full HTML document for an error-report email.

    :param project: Identifies the calling script/system.
    :param subject: Report headline, shown in the banner (unescaped input;
        rendered escaped).
    :param facts: Optional label/value rows for the facts table.
    :param error: Optional exception to render as a summary + traceback.
    :return: Full ``<html>`` document as a string.
    """
    sections: list[str] = []

    if facts:
        table = _facts_table_html(facts)
        if table:
            sections.append(table)

    if error is not None:
        summary, traceback_text = _format_exception(error)
        sections.append(
            "<div style='margin-top: 16px; font-weight: 600; "
            "color: #374151;'>Error</div>"
        )
        sections.append(_pre_block_html(summary))
        sections.append(
            "<div style='margin-top: 16px; font-weight: 600; "
            "color: #374151;'>Traceback</div>"
        )
        sections.append(_pre_block_html(traceback_text))

    body_sections = "".join(sections)

    return (
        "<html><body style='margin: 0; padding: 0; background: #f9fafb;'>"
        "<div style=\"font-family: -apple-system, BlinkMacSystemFont, "
        "'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #1f2937; "
        "font-size: 14px; line-height: 1.5; max-width: 720px; margin: 0 auto; "
        "background: #ffffff; padding: 0 0 24px 0;\">"
        "<div style='background: #b91c1c; color: #ffffff; "
        "padding: 16px 24px; font-size: 16px; font-weight: 600;'>"
        f"{_esc(subject)}"
        "</div>"
        "<div style='background: #f3f4f6; padding: 10px 24px; "
        "font-size: 13px; color: #374151; "
        "border-bottom: 1px solid #e5e7eb;'>"
        f"<span style='color: #6b7280;'>Project:</span> <strong>{_esc(project)}</strong>"
        "</div>"
        f"<div style='padding: 20px 24px;'>{body_sections}</div>"
        "<div style='padding: 12px 24px; color: #9ca3af; "
        "font-size: 12px; border-top: 1px solid #e5e7eb;'>"
        f"python · project: {_esc(project)}"
        "</div>"
        "</div>"
        "</body></html>"
    )
```

`#b91c1c` (red-700) is the fixed banner color, ported directly from `email.py`'s `_SEVERITY_COLOURS["error"]` — the only one of the three original severity colors this spec keeps, since this method is error-only.

### `MailServer.email_report` (new public method, replaces `email_reporting`)

```python
def email_report(
    self,
    sender: str,
    recipients: list[str] | str,
    subject: str,
    project: str,
    facts: list[tuple[str, Any]] | None = None,
    error: BaseException | None = None,
    cc: list[str] | str | None = None,
    bcc: list[str] | str | None = None,
    attachments: list[Path | tuple[str, bytes]] | None = None,
) -> None:
    """
    Send a styled HTML error-report email.

    :param sender: Sending address. Must be in the approved-senders list.
    :param recipients: One or more primary recipient addresses.
    :param subject: Report headline, shown in the banner. The sent email's
        subject line is this text prefixed with ``[python - {project}]``.
    :param project: Identifies the calling script/system (e.g. ``"api-ingest"``).
        Shown in the subject prefix, a strip under the banner, and the footer.
    :param facts: Optional ``(label, value)`` rows rendered as a table.
        Rows whose value is ``None``, ``""``, or ``[]`` are skipped.
    :param error: Optional exception. When given, its type name and message
        are shown as a one-line summary, and its full traceback is rendered
        below via :func:`traceback.format_exception`.
    :param cc: Carbon-copy recipients.
    :param bcc: Blind carbon-copy recipients.
    :param attachments: Files to attach. Same shape as :meth:`send_email`.
    :raises ValueError: If *sender* is not in the approved-senders list, or
        any address fails validation (raised by :meth:`send_email`).
    :raises FileNotFoundError: If a :class:`pathlib.Path` attachment does not exist.
    :raises TypeError: If an attachment item has an unexpected type.
    """
    logger.debug(
        "Sending error report — project: '%s', subject: '%s'.", project, subject,
    )
    body = _render_report_html(project, subject, facts, error)
    self.send_email(
        sender=sender,
        recipients=recipients,
        subject=f"[python - {project}] {subject}",
        body=body,
        body_type="html",
        cc=cc,
        bcc=bcc,
        attachments=attachments,
    )
```

`MailServer.email_reporting` is deleted.

### Imports

`wcp_library/emailing.py` currently has no `typing` import and does not import `html` or `traceback`. This work adds all three: `import html`, `import traceback`, and `from typing import Any` (needed for the `list[tuple[str, Any]]` facts type).

## API changes

### Added
- `MailServer.email_report(sender, recipients, subject, project, facts=None, error=None, cc=None, bcc=None, attachments=None) -> None`
- Private module-level helpers: `_esc`, `_facts_table_html`, `_pre_block_html`, `_format_exception`, `_render_report_html`.

### Removed
- `MailServer.email_reporting(subject, body) -> None` — no replacement shim. Callers must switch to `email_report` and now pass `sender`/`recipients` explicitly.

### Unchanged
- `send_email`, `_build_message`, `_send`, `_normalise_addresses`, `_build_attachment_part`, `EMAIL_PATTERN`, `_SMTP_SERVER`, `_SMTP_PORT`.

## Testing

Replace `TestEmailReporting` in `tests/emailing/test_emailing.py` with coverage for `email_report`, following the file's existing style (patch `send_email` on the instance, inspect call kwargs; construct a real `MailServer` via the `_make_mail_server()` helper already in the file):

- Facts table rendered into the HTML body when `facts` is given (assert escaped label/value text appears in the body passed to `send_email`).
- Rows with `None`/`""`/`[]` values are skipped from the facts table.
- No facts table markup when `facts` is omitted or empty.
- Error summary (`"ValueError: boom"`-style line) and traceback text both appear in the body when `error` is given.
- No error/traceback sections when `error` is omitted.
- Subject passed to `send_email` is `f"[python - {project}] {subject}"`.
- `body_type="html"` is passed to `send_email`.
- `cc`, `bcc`, and `attachments` are forwarded to `send_email` unchanged.
- `sender` and `recipients` are forwarded unchanged (no hardcoded address remains).

## Documentation

`docs/Wiki Docs/Helper ‐ Emailing`:
- Replace the `email_reporting()` section (signature, description, parameters, behavior) with `email_report()`'s equivalent, matching the file's existing format for `send_email()`.
- Update the top overview line ("A convenience wrapper `email_reporting()` for sending plain-text notifications to the internal Reporting distribution list.") to describe `email_report()` instead.
- Replace the "Send a reporting email" usage example at the bottom with an `email_report()` example, including an `error=` case (e.g. inside an `except` block).

## Open questions

None.
