import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path
from typing import Optional

from wcp_library.credentials.internet import InternetCredentialManager


class MailServer:
    def __init__(self, VAULT_API_KEY: str, SMTP2GO_PASSWORD_ID: int):
        self._approved_senders = ["python@wcap.ca", "workflow@wcap.ca"]

        _credential_manager = InternetCredentialManager(VAULT_API_KEY)
        _credentials = _credential_manager.get_credential_from_id(SMTP2GO_PASSWORD_ID)

        self._SMTP_USERNAME = _credentials["UserName"]
        self._SMTP_PASSWORD = _credentials["Password"]
        self._SMTP_SERVER_ADDRESS = "mail.smtp2go.com"
        self._SMTP_PORT = 587

    def send_email(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body: str,
        body_type: Optional[str] = "plain",
        attachments: Optional[list[Path]] = None,
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
    ) -> None:
        """
        Send an email with optional HTML formatting and attachments.

        :param sender: Email address of the sender
        :param recipients: List of recipient email addresses
        :param subject: Subject of the email
        :param body: Email body (plain text or HTML)
        :param body_type: 'plain' for text, 'html' for HTML content
        :param attachments: List of Path objects for attachments
        :param cc: List of CC email addresses
        :param bcc: List of BCC email addresses
        """

        if sender.lower() not in self._approved_senders:
            raise ValueError(f"Sender {sender} is not approved to send emails.")

        # Normalize optional parameters
        attachments = attachments or []
        cc = cc or []
        bcc = bcc or []

        # Create the email container
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        if cc:
            msg["Cc"] = ", ".join(cc)
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject

        # Attach the body (plain or HTML)
        if body_type not in ["plain", "html"]:
            raise ValueError("body_type must be either 'plain' or 'html'")
        msg.attach(MIMEText(body, body_type))

        # Attach files if provided
        if attachments:
            for attachment in attachments:
                if not isinstance(attachment, Path):
                    raise TypeError("attachments must be a list of Path objects")
                if not attachment.exists() or not attachment.is_file():
                    raise FileNotFoundError(f"Attachment not found: {attachment}")

                part = MIMEBase("application", "octet-stream")
                with open(attachment, "rb") as file:
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={attachment.name}")
                msg.attach(part)

        # Combine all recipients and remove duplicates
        all_recipients = list(dict.fromkeys([*recipients, *cc, *bcc]))

        # Create fresh connection for each send
        with smtplib.SMTP(self._SMTP_SERVER_ADDRESS, self._SMTP_PORT) as server:
            server.starttls()
            server.login(self._SMTP_USERNAME, self._SMTP_PASSWORD)
            server.sendmail(sender, all_recipients, msg.as_string())


    def email_reporting(self, subject: str, body: str) -> None:
        """
        Function to email the reporting team from the Python email

        :param subject: Subject of the email
        :param body: Body of the email
        :return:
        """

        self.send_email(
            sender="Python@wcap.ca",
            recipients=["Reporting@wcap.ca"],
            subject=subject,
            body=body,
        )
