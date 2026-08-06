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

# Simple email validation regex
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

_SMTP_SERVER: str = "mail.smtp2go.com"
_SMTP_PORT: int = 587

logger = logging.getLogger(__name__)


class MailServer:
    def __init__(self, internet_password_key: str, smtp2go_credential_id: int) -> None:
        self._approved_senders = ["python@wcap.ca", "workflow@wcap.ca", "reports@wcap.ca"]
        logger.debug(
            "Fetching SMTP2GO credentials from vault (entry ID: %d).",
            smtp2go_credential_id,
        )
        credential_manager = InternetCredentialManager(internet_password_key)
        credentials = credential_manager.get_credential_from_id(smtp2go_credential_id)

        self._smtp_username: str = credentials["UserName"]
        self._smtp_password: str = credentials["Password"]
        logger.debug("MailServer initialised for SMTP user '%s'.", self._smtp_username)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def send_email(
        self,
        sender: str,
        recipients: list[str] | str,
        subject: str,
        body: str,
        body_type: str = "plain",
        attachments: list[Path | tuple[str, bytes]] | None = None,
        cc: list[str] | str | None = None,
        bcc: list[str] | str | None = None,
    ) -> None:
        """
        Send an email with optional attachments and CC / BCC recipients.

        :param sender: Sending address. Must be in the approved-senders list.
        :param recipients: One or more primary recipient addresses.
        :param subject: Email subject line.
        :param body: Email body — plain text or HTML depending on *body_type*.
        :param body_type: ``"plain"`` (default) or ``"html"``.
        :param attachments: Files to attach. Each item is either a :class:`pathlib.Path`
            to an existing file, or a ``(filename, raw_bytes)`` tuple.
        :param cc: Carbon-copy recipients.
        :param bcc: Blind carbon-copy recipients.
        :raises ValueError: If *sender* is not in the approved-senders list, or
            *body_type* is not ``"plain"`` or ``"html"``.
        :raises FileNotFoundError: If a :class:`pathlib.Path` attachment does not exist.
        :raises TypeError: If an attachment item has an unexpected type.
        """
        logger.debug("Preparing email — subject: '%s', sender: '%s'.", subject, sender)

        if sender.lower() not in self._approved_senders:
            logger.error(
                "Rejected send attempt: '%s' is not an approved sender.", sender
            )
            raise ValueError(f"Sender '{sender}' is not approved to send emails.")

        # Validate email addresses
        def validate_email(email: str) -> bool:
            return bool(EMAIL_PATTERN.match(email))

        if not validate_email(sender):
            raise ValueError(f"Invalid sender email address: {sender}")

        # Normalize parameters
        recipients = _normalise_addresses(recipients)
        cc = _normalise_addresses(cc)
        bcc = _normalise_addresses(bcc)
        attachments = attachments or []

        for recipient in recipients:
            if not validate_email(recipient):
                raise ValueError(f"Invalid recipient email address: {recipient}")

        for email in cc:
            if not validate_email(email):
                raise ValueError(f"Invalid CC email address: {email}")

        for email in bcc:
            if not validate_email(email):
                raise ValueError(f"Invalid BCC email address: {email}")

        msg = self._build_message(sender, recipients, subject, body, body_type, cc)

        for attachment in attachments:
            part = _build_attachment_part(attachment)
            msg.attach(part)

        # De-duplicate while preserving order
        all_recipients = list(dict.fromkeys([*recipients, *cc, *bcc]))

        self._send(msg, sender, all_recipients)
        logger.info(
            "Email sent — subject: '%s' | from: %s | to: %s | attachments: %d.",
            subject,
            sender,
            recipients,
            len(attachments),
        )

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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_message(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body: str,
        body_type: str,
        cc: list[str],
    ) -> MIMEMultipart:
        """
        Construct a :class:`MIMEMultipart` message object.

        :param sender: Sending address.
        :param recipients: Normalised primary recipient list.
        :param subject: Email subject line.
        :param body: Email body text.
        :param body_type: ``"plain"`` or ``"html"``.
        :param cc: Normalised CC recipient list.
        :return: Fully assembled message, ready for attachments.
        """
        logger.debug("Building MIME message (body_type: '%s').", body_type)

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = "; ".join(recipients)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject

        if cc:
            msg["Cc"] = "; ".join(cc)

        msg.attach(MIMEText(body, body_type))
        return msg

    def _send(self, msg: MIMEMultipart, sender: str, recipients: list[str]) -> None:
        """
        Open a fresh SMTP connection and deliver *msg*.

        :param msg: The fully constructed message object.
        :param sender: Envelope-from address.
        :param recipients: All envelope-to addresses (To + Cc + Bcc combined).
        :raises smtplib.SMTPException: Re-raised after logging if any SMTP-level error occurs.
        """
        logger.debug(
            "Opening SMTP connection to %s:%d.", _SMTP_SERVER, _SMTP_PORT
        )
        try:
            with smtplib.SMTP(_SMTP_SERVER, _SMTP_PORT) as server:
                server.starttls()
                logger.debug(
                    "STARTTLS negotiated; logging in as '%s'.", self._smtp_username
                )
                server.login(self._smtp_username, self._smtp_password)
                server.sendmail(sender, recipients, msg.as_string())
                logger.debug(
                    "SMTP sendmail completed for %d recipient(s).", len(recipients)
                )
        except smtplib.SMTPException:
            logger.exception(
                "SMTP error while sending to %s via %s:%d.",
                recipients,
                _SMTP_SERVER,
                _SMTP_PORT,
            )
            raise


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _normalise_addresses(addresses: list[str] | str | None) -> list[str]:
    """
    Coerce an address argument into a plain list.

    :param addresses: A single address string, a list of addresses, or ``None``.
    :return: Always a list; empty when *addresses* is ``None``.
    """
    if addresses is None:
        return []
    if isinstance(addresses, str):
        return [addresses]
    return list(addresses)


def _build_attachment_part(attachment: Path | tuple[str, bytes]) -> MIMEBase:
    """
    Create a :class:`MIMEBase` part from a file path or raw-bytes tuple.

    :param attachment: Either a :class:`pathlib.Path` pointing to an existing file,
        or a ``(filename, raw_bytes)`` tuple.
    :return: Base64-encoded MIME part with ``Content-Disposition`` set.
    :raises FileNotFoundError: If *attachment* is a :class:`pathlib.Path` that does not exist.
    :raises TypeError: If *attachment* is not a recognised type.
    """
    part = MIMEBase("application", "octet-stream")

    if isinstance(attachment, Path):
        if not attachment.is_file():
            logger.error(
                "Attachment path not found or is not a file: '%s'.", attachment
            )
            raise FileNotFoundError(f"Attachment not found: {attachment}")
        logger.debug(
            "Attaching file from path: '%s' (%d bytes).",
            attachment,
            attachment.stat().st_size,
        )
        part.set_payload(attachment.read_bytes())
        filename = attachment.name

    elif (
        isinstance(attachment, tuple)
        and len(attachment) == 2
        and isinstance(attachment[0], str)
        and isinstance(attachment[1], bytes)
    ):
        filename, file_data = attachment
        logger.debug(
            "Attaching in-memory file: '%s' (%d bytes).", filename, len(file_data)
        )
        part.set_payload(file_data)

    else:
        logger.error(
            "Invalid attachment type '%s'; expected Path or (str, bytes) tuple.",
            type(attachment).__name__,
        )
        raise TypeError(
            "Each attachment must be a Path or a (filename: str, data: bytes) tuple."
        )

    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={filename}")
    return part


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
        if value is None or (isinstance(value, (str, list)) and not value):
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
