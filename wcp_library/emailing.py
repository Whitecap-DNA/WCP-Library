import smtplib
import warnings
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path


def send_email(
    sender: str,
    recipients: list[str],
    subject: str,
    body: str,
    body_type: str = "plain",
    attachments: list[Path] = None,
) -> None:
    """
    Send an email with optional HTML formatting and attachments.

    :param sender: Email address of the sender
    :param recipients: List of recipient email addresses
    :param subject: Subject of the email
    :param body: Email body (plain text or HTML)
    :param body_type: 'plain' for text, 'html' for HTML content
    :param attachments: List of Path objects for attachments
    """

    # Create the email container
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject

    # Attach the body (plain or HTML)
    msg.attach(MIMEText(body, body_type))

    # Attach files if provided
    if attachments:
        for attachment in attachments:
            part = MIMEBase("application", "octet-stream")
            with open(attachment, "rb") as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", f"attachment; filename={attachment.name}"
            )
            msg.attach(part)

    # Send the email
    smtp_server = "mail.wcap.ca"
    with smtplib.SMTP(smtp_server, 25) as server:
        server.ehlo()
        server.sendmail(sender, recipients, msg.as_string())


def email_reporting(subject: str, body: str) -> None:
    """
    Function to email the reporting team from the Python email

    :param subject: Subject of the email
    :param body: Body of the email
    :return:
    """

    send_email(
        sender="Python@wcap.ca",
        recipients=["Reporting@wcap.ca"],
        subject=subject,
        body=body,
    )


def send_html_email(
    sender: str, recipients: list, subject: str, html_content: str
) -> None:
    """
    ***DEPRECATED: Please don't use this function!
    send_email will handle an html body with the parameter body_type='html'***

    :param sender:
    :param recipients:
    :param subject:
    :param html_content:
    :return:
    """

    warnings.warn("send_html_email is deprecated. Please use send_email with body_type='html' instead.", DeprecationWarning)
    send_email(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=html_content,
        body_type="html",
    )


def email_with_attachments(
    sender: str,
    recipients: list,
    subject: str,
    message: str = None,
    attachments: list[Path] = None,
) -> None:
    """
    ***DEPRECATED: Please don't use this function!
    send_email will handle attachments the same way this function did.***

    :param sender:
    :param recipients:
    :param subject:
    :param message:
    :param attachments:
    :return:
    """

    warnings.warn("email_with_attachments is deprecated. Please use send_email instead.", DeprecationWarning)
    send_email(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=message,
        attachments=attachments,
    )
