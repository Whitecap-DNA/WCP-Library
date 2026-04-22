"""Mock tests for wcp_library.credentials.api."""
from unittest.mock import MagicMock, patch

import pytest

from wcp_library.credentials.api import (
    APICredentialManager,
    AsyncAPICredentialManager,
)


def _sample_creds(**overrides):
    base = {
        "UserName": "myuser",
        "Password": "mypass",
        "API KEY": "abc123",
        "Authentication Header": "Bearer",
        "URL": "https://api.example.com",
    }
    base.update(overrides)
    return base


class TestAPICredentialManagerConstruction:
    def test_password_list_id_is_214(self):
        mgr = APICredentialManager("k")
        assert mgr._password_list_id == 214
        assert mgr.api_key == "k"


class TestAPINewCredentials:
    def test_happy_path_calls_publish_and_builds_payload(self):
        mgr = APICredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample_creds())
        assert ok is True
        mock_pub.assert_called_once()
        payload = mock_pub.call_args.args[0]
        assert payload["PasswordListID"] == 214
        assert payload["Title"] == "MYUSER"  # uppercased UserName
        assert payload["Notes"] is None
        assert payload["UserName"] == "myuser"
        assert payload["Password"] == "mypass"
        assert payload["GenericField1"] == "abc123"
        assert payload["GenericField2"] == "Bearer"
        assert payload["URL"] == "https://api.example.com"

    def test_explicit_title_is_uppercased(self):
        mgr = APICredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample_creds(Title="custom-title"))
        assert mock_pub.call_args.args[0]["Title"] == "CUSTOM-TITLE"

    def test_notes_carried_through(self):
        mgr = APICredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample_creds(Notes="hi there"))
        assert mock_pub.call_args.args[0]["Notes"] == "hi there"

    def test_missing_required_key_raises_keyerror(self):
        mgr = APICredentialManager("k")
        creds = _sample_creds()
        creds.pop("Password")
        with patch.object(mgr, "_publish_new_password"):
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_returns_false_propagates(self):
        mgr = APICredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample_creds()) is False


class TestAsyncAPICredentialManagerConstruction:
    def test_password_list_id_is_214(self):
        mgr = AsyncAPICredentialManager("k")
        assert mgr._password_list_id == 214


class TestAsyncAPINewCredentials:
    async def test_happy_path(self):
        mgr = AsyncAPICredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample_creds())
        assert ok is True
        payload = fake_publish.payload
        assert payload["PasswordListID"] == 214
        assert payload["Title"] == "MYUSER"
        assert payload["GenericField1"] == "abc123"
        assert payload["GenericField2"] == "Bearer"
        assert payload["URL"] == "https://api.example.com"

    async def test_title_override(self):
        mgr = AsyncAPICredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            await mgr.new_credentials(_sample_creds(Title="admin-key"))
        assert fake_publish.payload["Title"] == "ADMIN-KEY"

    async def test_missing_key_raises(self):
        mgr = AsyncAPICredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample_creds()
        creds.pop("URL")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)
