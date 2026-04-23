"""Mock tests for wcp_library.graph.mail.

All HTTP calls are patched via unittest.mock. No network access occurs.
``aiofiles.open`` is patched for save_attachment so no real file IO occurs.
"""
import base64
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from wcp_library.graph import mail


# --------------------------- Helpers --------------------------- #


def _ok_json(payload, status_code=200):
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = payload
    mock.status_code = status_code
    return mock


def _http_error(status=500):
    err = requests.exceptions.RequestException("boom")
    err.response = MagicMock(status_code=status, text="error body")
    return err


HEADERS = {"Authorization": "Bearer testtoken"}
MAILBOX = "user@example.com"
MESSAGE_ID = "AAMkAGI1AAAoZDaBAAA="
FOLDER_ID = "AQMkADA4NGJlZGQxLTAAAA="


# ======================= Mailbox Functions ======================= #


class TestGetMailboxFolders:
    def test_lists_root_mail_folders(self):
        payload = {"value": [{"id": "f1", "displayName": "Inbox"}]}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            result = mail.get_mailbox_folders(HEADERS, MAILBOX)
            assert result == [{"id": "f1", "displayName": "Inbox"}]
            called_method = mock_request.call_args[0][0]
            called_url = mock_request.call_args[0][1]
            assert called_method == "GET"
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/mailFolders"
            )
            assert mock_request.call_args[0][2] == HEADERS

    def test_lists_child_folders_when_parent_provided(self):
        payload = {"value": [{"id": "child", "displayName": "Subfolder"}]}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            mail.get_mailbox_folders(HEADERS, MAILBOX, parent_folder_id="parent-1")
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/mailFolders"
                "/parent-1/childFolders"
            )

    def test_returns_empty_list_on_request_exception(self):
        with patch(
            "wcp_library.graph.mail._request", side_effect=_http_error()
        ):
            assert mail.get_mailbox_folders(HEADERS, MAILBOX) == []

    def test_returns_empty_list_when_value_missing(self):
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json({})
        ):
            assert mail.get_mailbox_folders(HEADERS, MAILBOX) == []


# ======================= Email Functions ======================= #


class TestParseEmailNotification:
    def test_returns_mailbox_and_message_id(self):
        notification = {
            "resource": f"users/{MAILBOX}/messages/{MESSAGE_ID}",
        }
        mailbox, message_id = mail.parse_email_notification(notification)
        assert mailbox == MAILBOX
        assert message_id == MESSAGE_ID

    def test_raises_value_error_on_malformed_resource(self):
        with pytest.raises(ValueError, match="Malformed resource data"):
            mail.parse_email_notification({"resource": "users/only"})

    def test_raises_value_error_when_resource_missing(self):
        with pytest.raises(ValueError, match="Malformed resource data"):
            mail.parse_email_notification({})


class TestGetEmailMetadata:
    def test_returns_message_json(self):
        payload = {"id": MESSAGE_ID, "subject": "Hello"}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            result = mail.get_email_metadata(HEADERS, MAILBOX, MESSAGE_ID)
            assert result == payload
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages/{MESSAGE_ID}"
            )
            assert mock_request.call_args[0][2] == HEADERS

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.mail._request", side_effect=_http_error()
        ):
            assert mail.get_email_metadata(HEADERS, MAILBOX, MESSAGE_ID) is None


class TestGetEmails:
    def test_lists_emails_from_root_when_no_folder_id(self):
        payload = {"value": [{"id": MESSAGE_ID, "subject": "Hi"}]}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            result = mail.get_emails(HEADERS, MAILBOX)
            assert result == [{"id": MESSAGE_ID, "subject": "Hi"}]
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages"
            )

    def test_lists_emails_from_folder_when_id_provided(self):
        payload = {"value": []}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            mail.get_emails(HEADERS, MAILBOX, folder_id=FOLDER_ID)
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}"
                f"/mailFolders/{FOLDER_ID}/messages"
            )

    def test_returns_empty_list_on_request_exception(self):
        with patch(
            "wcp_library.graph.mail._request", side_effect=_http_error()
        ):
            assert mail.get_emails(HEADERS, MAILBOX) == []

    def test_returns_empty_list_when_value_missing(self):
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json({})
        ):
            assert mail.get_emails(HEADERS, MAILBOX) == []


class TestGetAttachments:
    def test_returns_enriched_attachments_with_name_parts(self):
        payload = {
            "value": [
                {"id": "att-1", "name": "report.xlsx", "contentBytes": "xxx"},
                {"id": "att-2", "name": "photo.jpeg", "contentBytes": "yyy"},
            ]
        }
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ) as mock_request:
            result = mail.get_attachments(HEADERS, MAILBOX, MESSAGE_ID)
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/users/{MAILBOX}"
                f"/messages/{MESSAGE_ID}/attachments"
            )
            assert result == [
                {
                    "id": "att-1",
                    "name": "report.xlsx",
                    "contentBytes": "xxx",
                    "name_no_extension": "report",
                    "extension": "xlsx",
                },
                {
                    "id": "att-2",
                    "name": "photo.jpeg",
                    "contentBytes": "yyy",
                    "name_no_extension": "photo",
                    "extension": "jpeg",
                },
            ]

    def test_handles_attachment_without_name(self):
        payload = {"value": [{"id": "att-no-name"}]}
        with patch(
            "wcp_library.graph.mail._request", return_value=_ok_json(payload)
        ):
            result = mail.get_attachments(HEADERS, MAILBOX, MESSAGE_ID)
            assert result[0]["name_no_extension"] == ""
            assert result[0]["extension"] == ""

    def test_returns_empty_list_on_request_exception(self):
        with patch(
            "wcp_library.graph.mail._request", side_effect=_http_error()
        ):
            assert mail.get_attachments(HEADERS, MAILBOX, MESSAGE_ID) == []


class TestSaveAttachment:
    def test_writes_base64_decoded_bytes_from_dict(self, tmp_path):
        content_b64 = base64.b64encode(b"hello world").decode()
        attachment = {"contentBytes": content_b64, "name": "x.txt"}
        fake_file = AsyncMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=fake_file)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        destination = tmp_path / "x.txt"
        with patch(
            "wcp_library.graph.mail.aiofiles.open", return_value=mock_cm
        ) as mock_open:
            mail.save_attachment(attachment, destination)
            mock_open.assert_called_once_with(destination, "wb")
            fake_file.write.assert_awaited_once_with(b"hello world")

    def test_writes_bytes_directly_when_source_is_bytes(self, tmp_path):
        raw = b"raw-bytes"
        fake_file = AsyncMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=fake_file)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        destination = tmp_path / "raw.bin"
        with patch(
            "wcp_library.graph.mail.aiofiles.open", return_value=mock_cm
        ) as mock_open:
            mail.save_attachment(raw, destination)
            mock_open.assert_called_once_with(destination, "wb")
            fake_file.write.assert_awaited_once_with(b"raw-bytes")

    def test_raises_type_error_on_unsupported_source(self, tmp_path):
        with pytest.raises(TypeError, match="Source must be bytes or dict"):
            mail.save_attachment(12345, tmp_path / "x.txt")

    def test_missing_content_bytes_defaults_to_empty(self, tmp_path):
        fake_file = AsyncMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=fake_file)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        destination = tmp_path / "empty.bin"
        with patch(
            "wcp_library.graph.mail.aiofiles.open", return_value=mock_cm
        ):
            # base64.b64decode(b"") == b""  -> no error
            mail.save_attachment({"name": "nope"}, destination)
            fake_file.write.assert_awaited_once_with(b"")
