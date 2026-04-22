"""Mock tests for wcp_library.credentials.ftp."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.ftp import (
    AsyncFTPCredentialManager,
    FTPCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "BoB",
        "Password": "secret",
        "Host": "sftp.example.com",
        "Port": 22,
        "FTP/SFTP": "SFTP",
    }
    base.update(overrides)
    return base


class TestFTPConstruction:
    def test_password_list_id_is_208(self):
        mgr = FTPCredentialManager("k")
        assert mgr._password_list_id == 208


class TestFTPNewCredentials:
    def test_happy_path_payload(self):
        mgr = FTPCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 208
        assert p["Title"] == "BOB"            # upper from UserName
        assert p["UserName"] == "bob"         # lower from UserName
        assert p["Password"] == "secret"
        assert p["GenericField1"] == "sftp.example.com"
        assert p["GenericField2"] == 22
        assert p["GenericField3"] == "SFTP"
        assert p["Notes"] is None

    def test_title_override_uppercased(self):
        mgr = FTPCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="custom"))
        assert mock_pub.call_args.args[0]["Title"] == "CUSTOM"

    def test_notes_carried(self):
        mgr = FTPCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Notes="n"))
        assert mock_pub.call_args.args[0]["Notes"] == "n"

    def test_missing_host_raises(self):
        mgr = FTPCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("Host")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_false_propagates(self):
        mgr = FTPCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncFTPConstruction:
    def test_password_list_id_is_208(self):
        assert AsyncFTPCredentialManager("k")._password_list_id == 208


class TestAsyncFTPNewCredentials:
    async def test_happy_path(self):
        mgr = AsyncFTPCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["PasswordListID"] == 208
        assert p["Title"] == "BOB"
        assert p["UserName"] == "bob"
        assert p["GenericField3"] == "SFTP"

    async def test_missing_port_raises(self):
        mgr = AsyncFTPCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Port")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)
