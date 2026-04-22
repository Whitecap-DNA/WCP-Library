"""Tests for wcp_library.emailing."""
from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mail_server() -> "object":
    """Create a MailServer with its credential fetch patched out."""
    fake_credentials = {"UserName": "smtp-user", "Password": "smtp-pass"}
    with patch(
        "wcp_library.emailing.InternetCredentialManager"
    ) as mock_manager_cls:
        mock_manager = MagicMock()
        mock_manager.get_credential_from_id.return_value = fake_credentials
        mock_manager_cls.return_value = mock_manager

        from wcp_library.emailing import MailServer

        return MailServer(internet_password_key="dummy-key", smtp2go_credential_id=42)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


class TestNormaliseAddresses:
    def test_none_returns_empty_list(self) -> None:
        from wcp_library.emailing import _normalise_addresses

        assert _normalise_addresses(None) == []

    def test_string_becomes_single_item_list(self) -> None:
        from wcp_library.emailing import _normalise_addresses

        assert _normalise_addresses("a@b.com") == ["a@b.com"]

    def test_list_is_returned_as_list(self) -> None:
        from wcp_library.emailing import _normalise_addresses

        source = ["a@b.com", "c@d.com"]
        result = _normalise_addresses(source)
        assert result == source
        # Should be a new list (safe to mutate)
        assert result is not source


class TestBuildAttachmentPart:
    def test_bytes_tuple_attachment(self) -> None:
        from wcp_library.emailing import _build_attachment_part

        part = _build_attachment_part(("hello.txt", b"hello world"))
        assert part.get_filename() is None or "hello.txt" in part["Content-Disposition"]
        assert "hello.txt" in part["Content-Disposition"]

    def test_path_attachment_reads_file(self, tmp_path: Path) -> None:
        from wcp_library.emailing import _build_attachment_part

        file_path = tmp_path / "data.bin"
        file_path.write_bytes(b"\x00\x01\x02")

        part = _build_attachment_part(file_path)
        assert "data.bin" in part["Content-Disposition"]

    def test_missing_path_raises_filenotfound(self, tmp_path: Path) -> None:
        from wcp_library.emailing import _build_attachment_part

        missing = tmp_path / "does-not-exist.txt"
        with pytest.raises(FileNotFoundError):
            _build_attachment_part(missing)

    def test_invalid_type_raises_typeerror(self) -> None:
        from wcp_library.emailing import _build_attachment_part

        with pytest.raises(TypeError):
            _build_attachment_part(12345)  # type: ignore[arg-type]

    def test_malformed_tuple_raises_typeerror(self) -> None:
        from wcp_library.emailing import _build_attachment_part

        with pytest.raises(TypeError):
            _build_attachment_part(("name",))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# MailServer construction
# ---------------------------------------------------------------------------


class TestMailServerInit:
    def test_credentials_pulled_from_vault(self) -> None:
        with patch(
            "wcp_library.emailing.InternetCredentialManager"
        ) as mock_manager_cls:
            mock_manager = MagicMock()
            mock_manager.get_credential_from_id.return_value = {
                "UserName": "u",
                "Password": "p",
            }
            mock_manager_cls.return_value = mock_manager

            from wcp_library.emailing import MailServer

            server = MailServer(internet_password_key="key", smtp2go_credential_id=7)

            mock_manager_cls.assert_called_once_with("key")
            mock_manager.get_credential_from_id.assert_called_once_with(7)
            assert server._smtp_username == "u"
            assert server._smtp_password == "p"


# ---------------------------------------------------------------------------
# MailServer.send_email
# ---------------------------------------------------------------------------


class TestSendEmail:
    def test_sendmail_called_with_expected_args(self) -> None:
        server = _make_mail_server()

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            smtp_instance = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = smtp_instance

            server.send_email(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="Hi",
                body="Body",
            )

            smtp_instance.starttls.assert_called_once()
            smtp_instance.login.assert_called_once_with("smtp-user", "smtp-pass")
            smtp_instance.sendmail.assert_called_once()

            envelope_from, envelope_to, raw_msg = smtp_instance.sendmail.call_args.args
            assert envelope_from == "python@wcap.ca"
            assert envelope_to == ["to@example.com"]
            assert "Subject: Hi" in raw_msg

    def test_cc_and_bcc_added_to_envelope(self) -> None:
        server = _make_mail_server()

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            smtp_instance = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = smtp_instance

            server.send_email(
                sender="python@wcap.ca",
                recipients="to@example.com",
                subject="Subj",
                body="Body",
                cc="cc@example.com",
                bcc=["bcc@example.com"],
            )

            _, envelope_to, raw_msg = smtp_instance.sendmail.call_args.args
            assert "to@example.com" in envelope_to
            assert "cc@example.com" in envelope_to
            assert "bcc@example.com" in envelope_to
            # BCC must not appear as a header
            assert "Bcc:" not in raw_msg

    def test_disallowed_sender_raises_valueerror(self) -> None:
        server = _make_mail_server()

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            with pytest.raises(ValueError):
                server.send_email(
                    sender="stranger@wcap.ca",
                    recipients=["to@example.com"],
                    subject="s",
                    body="b",
                )
            mock_smtp_cls.assert_not_called()

    def test_invalid_recipient_raises_valueerror(self) -> None:
        server = _make_mail_server()

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            with pytest.raises(ValueError):
                server.send_email(
                    sender="python@wcap.ca",
                    recipients=["not-an-email"],
                    subject="s",
                    body="b",
                )
            mock_smtp_cls.assert_not_called()

    def test_smtp_exception_is_reraised(self) -> None:
        server = _make_mail_server()

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            smtp_instance = MagicMock()
            smtp_instance.sendmail.side_effect = smtplib.SMTPException("boom")
            mock_smtp_cls.return_value.__enter__.return_value = smtp_instance

            with pytest.raises(smtplib.SMTPException):
                server.send_email(
                    sender="python@wcap.ca",
                    recipients=["to@example.com"],
                    subject="s",
                    body="b",
                )

    def test_attachment_from_path_is_sent(self, tmp_path: Path) -> None:
        server = _make_mail_server()
        attachment = tmp_path / "note.txt"
        attachment.write_text("hello")

        with patch("wcp_library.emailing.smtplib.SMTP") as mock_smtp_cls:
            smtp_instance = MagicMock()
            mock_smtp_cls.return_value.__enter__.return_value = smtp_instance

            server.send_email(
                sender="python@wcap.ca",
                recipients=["to@example.com"],
                subject="s",
                body="b",
                attachments=[attachment],
            )

            _, _, raw_msg = smtp_instance.sendmail.call_args.args
            assert "note.txt" in raw_msg


# ---------------------------------------------------------------------------
# MailServer.email_reporting
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# MailServer._build_message
# ---------------------------------------------------------------------------


class TestBuildMessage:
    def test_headers_and_cc_populated(self) -> None:
        server = _make_mail_server()
        msg = server._build_message(
            sender="python@wcap.ca",
            recipients=["to@example.com", "second@example.com"],
            subject="Sub",
            body="Body",
            body_type="plain",
            cc=["cc@example.com"],
        )
        assert isinstance(msg, MIMEMultipart)
        assert msg["From"] == "python@wcap.ca"
        assert "to@example.com" in msg["To"]
        assert "second@example.com" in msg["To"]
        assert msg["Subject"] == "Sub"
        assert msg["Cc"] == "cc@example.com"

    def test_no_cc_header_when_cc_empty(self) -> None:
        server = _make_mail_server()
        msg = server._build_message(
            sender="python@wcap.ca",
            recipients=["to@example.com"],
            subject="Sub",
            body="Body",
            body_type="plain",
            cc=[],
        )
        assert msg["Cc"] is None
