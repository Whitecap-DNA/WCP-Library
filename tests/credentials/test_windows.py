"""Mock tests for wcp_library.credentials.windows."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.windows import (
    AsyncWindowsCredentialManager,
    WindowsCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "Alice",
        "Password": "hunter2",
    }
    base.update(overrides)
    return base


class TestWindowsConstruction:
    def test_password_list_id_is_91(self):
        mgr = WindowsCredentialManager("k")
        assert mgr._password_list_id == 91


class TestWindowsNewCredentials:
    def test_happy_path_payload(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 91
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"  # lowered
        assert p["Password"] == "hunter2"
        assert p["Notes"] is None

    def test_title_override(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="workstation"))
        assert mock_pub.call_args.args[0]["Title"] == "WORKSTATION"

    def test_notes_included(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Notes="some note"))
        assert mock_pub.call_args.args[0]["Notes"] == "some note"

    def test_missing_password_raises(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("Password")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_false_propagates(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncWindowsConstruction:
    def test_password_list_id_is_91(self):
        assert AsyncWindowsCredentialManager("k")._password_list_id == 91


class TestAsyncWindowsNewCredentials:
    async def test_happy_path(self):
        mgr = AsyncWindowsCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"

    async def test_missing_password_raises(self):
        mgr = AsyncWindowsCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Password")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)