import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path


def send_email(sender: str, recipients: list, subject: str, message: str=None) -> None:
    """
    Function to send an email

    :param sender:
    :param recipients:
    :param subject:
    :param message:
    :return:
    """

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(message))

    smtpServer = 'mail.wcap.ca'
    server = smtplib.SMTP(smtpServer, 25)
    server.ehlo()
    server.sendmail(sender, recipients, msg.as_string())
    server.quit()


def email_reporting(subject: str, message: str) -> None:
    """
    Function to email the reporting team from the Python email

    :param subject:
    :param message:
    :return:
    """

    msg = MIMEMultipart()
    msg['From'] = "Python@wcap.ca"
    msg['To'] = "Reporting@wcap.ca"
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(message))

    smtpServer = 'mail.wcap.ca'
    server = smtplib.SMTP(smtpServer, 25)
    server.ehlo()
    server.sendmail("Python@wcap.ca", 'Reporting@wcap.ca', msg.as_string())
    server.quit()


def email_with_attachments(sender: str, recipients: list, subject: str, message: str=None, attachments: list[Path]=None) -> None:
    """
    Function to send an email with attachments

    File paths must be passed as a list of Path (pathlib.Path) objects

    :param sender:
    :param recipients:
    :param subject:
    :param message:
    :param attachments:
    :return:
    """

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(message))

    for attachment in attachments:
        part = MIMEBase('application', "octet-stream")
        with open(attachment, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename={}'.format(attachment.name))
        msg.attach(part)

    smtpServer = 'mail.wcap.ca'
    server = smtplib.SMTP(smtpServer, 25)
    server.ehlo()
    server.sendmail(sender, recipients, msg.as_string())
    server.quit()