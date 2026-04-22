"""Mock tests for wcp_library.credentials.internet."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.internet import (
    AsyncInternetCredentialManager,
    InternetCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "Alice",
        "Password": "hunter2",
        "URL": "https://site.example.com",
    }
    base.update(overrides)
    return base


class TestInternetConstruction:
    def test_password_list_id_is_93(self):
        mgr = InternetCredentialManager("k")
        assert mgr._password_list_id == 93


class TestInternetNewCredentials:
    def test_happy_path_payload(self):
        mgr = InternetCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 93
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"  # lowered
        assert p["Password"] == "hunter2"
        assert p["URL"] == "https://site.example.com"
        assert p["Notes"] is None

    def test_title_override(self):
        mgr = InternetCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="portal"))
        assert mock_pub.call_args.args[0]["Title"] == "PORTAL"

    def test_notes_included(self):
        mgr = InternetCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Notes="some note"))
        assert mock_pub.call_args.args[0]["Notes"] == "some note"

    def test_missing_url_raises(self):
        mgr = InternetCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("URL")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_false_propagates(self):
        mgr = InternetCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncInternetConstruction:
    def test_password_list_id_is_93(self):
        assert AsyncInternetCredentialManager("k")._password_list_id == 93


class TestAsyncInternetNewCredentials:
    async def test_happy_path(self):
        mgr = AsyncInternetCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"
        assert p["URL"] == "https://site.example.com"

    async def test_missing_password_raises(self):
        mgr = AsyncInternetCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Password")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)
