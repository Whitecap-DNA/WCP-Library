"""SMTP email sending utilities backed by SMTP2GO."""

import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path

from wcp_library.credentials.internet import InternetCredentialManager

logger = logging.getLogger(__name__)

# Senders authorised to use this mail server
_APPROVED_SENDERS: frozenset[str] = frozenset({"python@wcap.ca", "workflow@wcap.ca"})
Attachment = Path | tuple[str, bytes]
EmailBodyType = str


class MailServer:
    """SMTP email client authenticated via SMTP2GO credentials.

    Parameters
    ----------
    internet_password_key : str
        API key used to retrieve credentials from the Pleasant Password vault.
    smtp2go_credential_id : int
        Vault entry ID for the SMTP2GO account credentials.
    """

    _SMTP_SERVER: str = "mail.smtp2go.com"
    _SMTP_PORT: int = 587

    def __init__(self, internet_password_key: str, smtp2go_credential_id: int) -> None:
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
        body_type: EmailBodyType = "plain",
        attachments: list[Attachment] | None = None,
        cc: list[str] | str | None = None,
        bcc: list[str] | str | None = None,
    ) -> None:
        """Send an email with optional attachments and CC / BCC recipients.

        Parameters
        ----------
        sender : str
            Sending address. Must be in the approved-senders list.
        recipients : list[str] | str
            One or more primary recipient addresses.
        subject : str
            Email subject line.
        body : str
            Email body — plain text or HTML depending on *body_type*.
        body_type : EmailBodyType, optional
            "plain" (default) or "html".
        attachments : list[Path | tuple[str, bytes]] | None, optional
            Files to attach. Each item is either a :class:`pathlib.Path` to an
            existing file, or a ``(filename, raw_bytes)`` tuple.
        cc : list[str] | str | None, optional
            Carbon-copy recipients.
        bcc : list[str] | str | None, optional
            Blind carbon-copy recipients.

        Raises
        ------
        ValueError
            If *sender* is not in the approved-senders list, or *body_type* is
            not "plain" or "html".
        FileNotFoundError
            If a :class:`pathlib.Path` attachment does not exist.
        TypeError
            If an attachment item has an unexpected type.
        """
        logger.debug("Preparing email — subject: '%s', sender: '%s'.", subject, sender)

        if sender.lower() not in _APPROVED_SENDERS:
            logger.error(
                "Rejected send attempt: '%s' is not an approved sender.", sender
            )
            raise ValueError(f"Sender '{sender}' is not approved to send emails.")

        if body_type not in {"plain", "html"}:
            logger.error(
                "Invalid body_type '%s'; must be 'plain' or 'html'.", body_type
            )
            raise ValueError("body_type must be 'plain' or 'html'.")

        recipients = _normalise_addresses(recipients)
        cc = _normalise_addresses(cc)
        bcc = _normalise_addresses(bcc)
        attachments = attachments or []

        logger.debug(
            "Recipients — To: %s | Cc: %s | Bcc: %s.",
            recipients,
            cc,
            ["<redacted>" for _ in bcc],  # BCC addresses not exposed in logs
        )

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

    def email_reporting(self, subject: str, body: str) -> None:
        """Send a plain-text email to the internal Reporting distribution list.

        Parameters
        ----------
        subject : str
            Email subject line.
        body : str
            Plain-text email body.
        """
        logger.debug("Sending reporting email — subject: '%s'.", subject)
        self.send_email(
            sender="python@wcap.ca",
            recipients=["Reporting@wcap.ca"],
            subject=subject,
            body=body,
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
        body_type: EmailBodyType,
        cc: list[str],
    ) -> MIMEMultipart:
        """Construct a :class:`MIMEMultipart` message object.

        Parameters
        ----------
        sender : str
            Sending address.
        recipients : list[str]
            Normalised primary recipient list.
        subject : str
            Email subject line.
        body : str
            Email body text.
        body_type : EmailBodyType
            ``"plain"`` or ``"html"``.
        cc : list[str]
            Normalised CC recipient list.

        Returns
        -------
        MIMEMultipart
            Fully assembled message, ready for attachments.
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
        """Open a fresh SMTP connection and deliver *msg*.

        Parameters
        ----------
        msg : MIMEMultipart
            The fully constructed message object.
        sender : str
            Envelope-from address.
        recipients : list[str]
            All envelope-to addresses (To + Cc + Bcc combined).

        Raises
        ------
        smtplib.SMTPException
            Re-raised after logging if any SMTP-level error occurs.
        """
        logger.debug(
            "Opening SMTP connection to %s:%d.", self._SMTP_SERVER, self._SMTP_PORT
        )
        try:
            with smtplib.SMTP(self._SMTP_SERVER, self._SMTP_PORT) as server:
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
                self._SMTP_SERVER,
                self._SMTP_PORT,
            )
            raise


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _normalise_addresses(addresses: list[str] | str | None) -> list[str]:
    """Coerce an address argument into a plain list.

    Parameters
    ----------
    addresses : list[str] | str | None
        A single address string, a list of addresses, or ``None``.

    Returns
    -------
    list[str]
        Always a list; empty when *addresses* is ``None``.
    """
    if addresses is None:
        return []
    if isinstance(addresses, str):
        return [addresses]
    return list(addresses)


def _build_attachment_part(attachment: Attachment) -> MIMEBase:
    """Create a :class:`MIMEBase` part from a file path or raw-bytes tuple.

    Parameters
    ----------
    attachment : Path | tuple[str, bytes]
        Either a :class:`pathlib.Path` pointing to an existing file, or a
        ``(filename, raw_bytes)`` tuple.

    Returns
    -------
    MIMEBase
        Base64-encoded MIME part with ``Content-Disposition`` set.

    Raises
    ------
    FileNotFoundError
        If *attachment* is a :class:`pathlib.Path` that does not exist.
    TypeError
        If *attachment* is not a recognised type.
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
