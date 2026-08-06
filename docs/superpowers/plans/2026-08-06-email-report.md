# Generic `email_report` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `MailServer.email_reporting(subject, body)` in `wcp_library/emailing.py` with a generic `MailServer.email_report(...)` that builds and sends a styled HTML error-report email (banner, project strip, optional facts table, optional error+traceback), following the design in `docs/superpowers/specs/2026-08-06-email-report-design.md`.

**Architecture:** New private module-level HTML-rendering helpers (`_esc`, `_facts_table_html`, `_pre_block_html`, `_format_exception`, `_render_report_html`) plus one new public method `MailServer.email_report`, all in `wcp_library/emailing.py`. `email_reporting` and its test class are deleted outright — no back-compat shim.

**Tech Stack:** Python, pytest (`unittest.mock.patch.object` on `send_email`, matching this test file's existing style).

## Global Constraints

- rST-style docstrings on all public methods and module-level functions (project CLAUDE.md convention) — `:param:`/`:return:`/`:raises:` fields, blank line preserved after the closing `"""`.
- No backward-compatibility shim for `email_reporting` — delete the method and its test class outright.
- No severity levels, no callout/highlight boxes, no multi-traceback deduplication — all explicitly out of scope per the spec.
- `email.py` at the repo root is a reference file only — not part of the `wcp_library` package, not imported anywhere in this repo. Do not modify or delete it.
- New imports required in `wcp_library/emailing.py`: `import html`, `import traceback`, `from typing import Any` — none of these are currently imported.
- The fixed banner color is `#b91c1c` (red-700), ported directly from the reference file's `_SEVERITY_COLOURS["error"]`.

---

### Task 1: `email_report` + helpers, replacing `email_reporting`

**Files:**
- Modify: `wcp_library/emailing.py`
- Modify: `tests/emailing/test_emailing.py`

**Interfaces:**
- Consumes: `MailServer.send_email(sender, recipients, subject, body, body_type="plain", attachments=None, cc=None, bcc=None) -> None` (already exists, unmodified — `email_report` forwards `sender`/`recipients`/`cc`/`bcc`/`attachments` to it verbatim and calls it with `body_type="html"`). Test helper `_make_mail_server()` already exists in the test file (constructs a `MailServer` with credential fetch patched out).
- Produces: `MailServer.email_report(sender: str, recipients: list[str] | str, subject: str, project: str, facts: list[tuple[str, Any]] | None = None, error: BaseException | None = None, cc: list[str] | str | None = None, bcc: list[str] | str | None = None, attachments: list[Path | tuple[str, bytes]] | None = None) -> None`. Nothing else in this task is consumed by Task 2 (Task 2 only touches documentation).

- [ ] **Step 1: Replace the `email_reporting` test class with `email_report` tests (failing)**

In `tests/emailing/test_emailing.py`, delete this entire class:

```python
class TestEmailReporting:
    def test_delegates_to_send_email(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_reporting("subj", "body")
            mock_send.assert_called_once()
            kwargs = mock_send.call_args.kwargs
            assert kwargs["sender"] == "python@wcap.ca"
            assert kwargs["recipients"] == ["Reporting@wcap.ca"]
            assert kwargs["subject"] == "subj"
            assert kwargs["body"] == "body"
```

Replace it (same location — the `# MailServer.email_reporting` section between `TestSendEmail` and `TestBuildMessage`) with:

```python
class TestEmailReport:
    def test_facts_table_rendered_when_given(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
                facts=[("Endpoint", "/invoices"), ("Table", "invoices")],
            )
            body = mock_send.call_args.kwargs["body"]
            assert "Endpoint" in body
            assert "/invoices" in body
            assert "Table" in body
            assert "invoices" in body

    def test_facts_with_empty_values_skipped(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
                facts=[
                    ("SkippedNone", None),
                    ("SkippedEmptyStr", ""),
                    ("SkippedEmptyList", []),
                    ("KeptLabel", "kept-value"),
                ],
            )
            body = mock_send.call_args.kwargs["body"]
            assert "KeptLabel" in body
            assert "kept-value" in body
            assert "SkippedNone" not in body
            assert "SkippedEmptyStr" not in body
            assert "SkippedEmptyList" not in body

    def test_no_facts_table_when_facts_omitted(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
            )
            body = mock_send.call_args.kwargs["body"]
            assert "<table" not in body

    def test_no_facts_table_when_all_rows_skipped(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
                facts=[("A", None), ("B", ""), ("C", [])],
            )
            body = mock_send.call_args.kwargs["body"]
            assert "<table" not in body

    def test_error_and_traceback_rendered_when_given(self) -> None:
        server = _make_mail_server()

        try:
            raise ValueError("boom")
        except ValueError as exc:
            error = exc

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
                error=error,
            )
            body = mock_send.call_args.kwargs["body"]
            assert "ValueError: boom" in body
            assert "Traceback (most recent call last)" in body

    def test_no_error_section_when_error_omitted(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
            )
            body = mock_send.call_args.kwargs["body"]
            assert "Traceback" not in body

    def test_subject_prefixed_with_project(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
            )
            assert (
                mock_send.call_args.kwargs["subject"]
                == "[python - api-ingest] Something broke"
            )

    def test_body_type_is_html(self) -> None:
        server = _make_mail_server()

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Something broke",
                project="api-ingest",
            )
            assert mock_send.call_args.kwargs["body_type"] == "html"

    def test_sender_recipients_cc_bcc_attachments_forwarded(
        self, tmp_path: Path
    ) -> None:
        server = _make_mail_server()
        attachment = tmp_path / "note.txt"
        attachment.write_text("hello")

        with patch.object(type(server), "send_email") as mock_send:
            server.email_report(
                sender="workflow@wcap.ca",
                recipients=["to@example.com", "second@example.com"],
                subject="Something broke",
                project="api-ingest",
                cc="cc@example.com",
                bcc=["bcc@example.com"],
                attachments=[attachment],
            )
            kwargs = mock_send.call_args.kwargs
            assert kwargs["sender"] == "workflow@wcap.ca"
            assert kwargs["recipients"] == ["to@example.com", "second@example.com"]
            assert kwargs["cc"] == "cc@example.com"
            assert kwargs["bcc"] == ["bcc@example.com"]
            assert kwargs["attachments"] == [attachment]
```

- [ ] **Step 2: Run the test file to verify the new tests fail**

Run: `poetry run pytest tests/emailing/test_emailing.py -v`
Expected: the 9 new `TestEmailReport` tests FAIL with `AttributeError: 'MailServer' object has no attribute 'email_report'`. All other tests in the file (`TestNormaliseAddresses`, `TestBuildAttachmentPart`, `TestMailServerInit`, `TestSendEmail`, `TestBuildMessage`) still PASS — this task does not touch anything they depend on.

- [ ] **Step 3: Add the new imports to `wcp_library/emailing.py`**

At the top of `wcp_library/emailing.py`, change:

```python
import logging
import re
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path

from wcp_library.credentials.internet import InternetCredentialManager
```

to:

```python
import html
import logging
import re
import smtplib
import traceback
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path
from typing import Any

from wcp_library.credentials.internet import InternetCredentialManager
```

- [ ] **Step 4: Delete `MailServer.email_reporting`**

Remove this entire method from the `MailServer` class in `wcp_library/emailing.py` (it sits between `send_email` and the `# Private helpers` section comment):

```python
    def email_reporting(self, subject: str, body: str) -> None:
        """
        Send a plain-text email to the internal Reporting distribution list.

        :param subject: Email subject line.
        :param body: Plain-text email body.
        """
        logger.debug("Sending reporting email — subject: '%s'.", subject)
        self.send_email(
            sender="python@wcap.ca",
            recipients=["Reporting@wcap.ca"],
            subject=subject,
            body=body,
        )
```

- [ ] **Step 5: Add `MailServer.email_report` in its place**

Insert this method exactly where `email_reporting` was removed from (between `send_email` and the `# Private helpers` section comment):

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

- [ ] **Step 6: Add the private rendering helpers**

Add these module-level functions to `wcp_library/emailing.py`, in the `# Module-level helpers` section at the bottom of the file (alongside `_normalise_addresses` and `_build_attachment_part`):

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
    :param subject: Report headline, shown in the banner (rendered escaped).
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

- [ ] **Step 7: Run the test file to verify everything passes**

Run: `poetry run pytest tests/emailing/test_emailing.py -v`
Expected: all tests PASS — the 9 new `TestEmailReport` tests plus every pre-existing test in the file (none of their behavior changed).

- [ ] **Step 8: Run the full test suite to confirm no regressions**

Run: `poetry run pytest`
Expected: all tests PASS. No other file references `email_reporting` (this task's diff is isolated to `wcp_library/emailing.py` and its test file), so no other test should be affected.

- [ ] **Step 9: Commit**

```bash
git add wcp_library/emailing.py tests/emailing/test_emailing.py
git commit -m "feat(emailing): replace email_reporting with generic email_report"
```

---

### Task 2: Update wiki docs

**Files:**
- Modify: `docs/Wiki Docs/Helper ‐ Emailing`

**Interfaces:**
- Consumes: `MailServer.email_report`'s signature and behavior from Task 1 (names/parameters only, to document accurately).
- Produces: nothing consumed by later tasks — this is the last task in the plan.

- [ ] **Step 1: Update the overview paragraph**

In `docs/Wiki Docs/Helper ‐ Emailing`, change:

```markdown
The module includes:

- A core method `send_email()` for sending emails with flexible options.
- A convenience wrapper `email_reporting()` for sending plain-text notifications to the internal Reporting distribution list.
```

to:

```markdown
The module includes:

- A core method `send_email()` for sending emails with flexible options.
- A generic `email_report()` for sending a styled HTML error-report email (facts table, optional exception summary + traceback).
```

- [ ] **Step 2: Replace the `email_reporting()` reference section**

Replace this entire section (between the `### email_reporting()` heading and the next `## Usage Examples` heading):

```markdown
### email_reporting()

#### Signature

```
email_reporting(subject: str, body: str) -> None
```

#### Description

Shortcut for sending a plain-text email to the internal Reporting distribution list.

#### Parameters

- `subject` (str): Email subject.
- `body` (str): Plain-text email body.

#### Behavior

- Uses `send_email()` internally.
- Sender: `python@wcap.ca`
- Recipient: `Reporting@wcap.ca`
```

with:

```markdown
### email_report()

#### Signature

```
email_report(
    sender: str,
    recipients: list[str] | str,
    subject: str,
    project: str,
    facts: list[tuple[str, Any]] | None = None,
    error: BaseException | None = None,
    cc: list[str] | str | None = None,
    bcc: list[str] | str | None = None,
    attachments: list[Path | tuple[str, bytes]] | None = None,
) -> None
```

#### Description

Sends a styled HTML error-report email: a red banner with the subject, a project-attribution strip, an optional facts table, and — when an exception is passed — its type/message summary and full traceback.

#### Parameters

- `sender` (str): Sending address. Must be in the approved-senders list (same list as `send_email()`).
- `recipients` (list[str] | str): One or more primary recipient addresses.
- `subject` (str): Report headline, shown in the banner. The email's actual subject line is this text prefixed with `[python - {project}]`.
- `project` (str): Identifies the calling script/system (e.g. `"api-ingest"`). Shown in the subject prefix, a strip under the banner, and the footer.
- `facts` (list[tuple[str, Any]] | None): Optional `(label, value)` rows rendered as a table. Rows whose value is `None`, `""`, or `[]` are skipped.
- `error` (BaseException | None): Optional exception. When given, its type name and message are shown as a one-line summary, and its full traceback is rendered below.
- `cc` (list[str] | str | None): Optional carbon-copy recipients.
- `bcc` (list[str] | str | None): Optional blind carbon-copy recipients.
- `attachments` (list[Path | tuple[str, bytes]] | None): Optional attachments — same shape as `send_email()`.

#### Behavior

- Uses `send_email()` internally, with `body_type="html"`.
- No hardcoded sender or recipient — both are passed in like `send_email()`.
```

- [ ] **Step 3: Replace the "Send a reporting email" usage example**

Replace this example at the bottom of the file:

```markdown
### Send a reporting email
```
mail_server = MailServer(<Vault-Internet-API-Key>, <SMTP2GO-Credential-ID>)
mail_server.email_reporting("Reporting report", "This is a log message")
```
```

with:

```markdown
### Send an error report

```
mail_server = MailServer(<Vault-Internet-API-Key>, <SMTP2GO-Credential-ID>)

try:
    run_pipeline()
except Exception as exc:
    mail_server.email_report(
        sender="python@wcap.ca",
        recipients=["Reporting@wcap.ca"],
        subject="Pipeline run failed",
        project="api-ingest",
        facts=[("Endpoint", "/invoices"), ("Table", "invoices")],
        error=exc,
    )
```
```

- [ ] **Step 4: Diff-check the file**

Run: `git diff "docs/Wiki Docs/Helper ‐ Emailing"`
Expected: the diff shows only the three replacements above — overview paragraph, `email_reporting()` → `email_report()` section, and the usage example — with no accidental changes elsewhere in the file.

- [ ] **Step 5: Commit**

```bash
git add "docs/Wiki Docs/Helper ‐ Emailing"
git commit -m "docs: document email_report, replacing email_reporting"
```
