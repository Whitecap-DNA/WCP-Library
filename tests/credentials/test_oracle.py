"""Mock tests for wcp_library.credentials.oracle."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.oracle import (
    AsyncOracleCredentialManager,
    OracleCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "OraUser",
        "Password": "pw",
        "Host": "oracle.example.com",
        "Port": 1521,
        "Service": "ORCLSVC",
    }
    base.update(overrides)
    return base


class TestOracleConstruction:
    def test_password_list_id_is_207(self):
        mgr = OracleCredentialManager("k")
        assert mgr._password_list_id == 207


class TestOracleNewCredentials:
    def test_happy_path_with_service(self):
        mgr = OracleCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 207
        assert p["Title"] == "ORAUSER"
        assert p["UserName"] == "orauser"
        assert p["Password"] == "pw"
        assert p["GenericField1"] == "oracle.example.com"
        assert p["GenericField2"] == 1521
        assert p["GenericField3"] == "ORCLSVC"
        assert p["GenericField4"] is None
        assert p["Notes"] is None

    def test_with_sid_only(self):
        mgr = OracleCredentialManager("k")
        creds = _sample()
        creds.pop("Service")
        creds["SID"] = "ORCLSID"
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(creds)
        p = mock_pub.call_args.args[0]
        assert p["GenericField3"] is None
        assert p["GenericField4"] == "ORCLSID"

    def test_missing_host_raises(self):
        mgr = OracleCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("Host")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_title_override(self):
        mgr = OracleCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="primary"))
        assert mock_pub.call_args.args[0]["Title"] == "PRIMARY"

    def test_publish_false_propagates(self):
        mgr = OracleCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncOracleConstruction:
    def test_password_list_id_is_207(self):
        assert AsyncOracleCredentialManager("k")._password_list_id == 207


class TestAsyncOracleNewCredentials:
    async def test_happy_path_service(self):
        mgr = AsyncOracleCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["UserName"] == "orauser"
        assert p["GenericField3"] == "ORCLSVC"
        assert p["GenericField4"] is None

    async def test_missing_port_raises(self):
        mgr = AsyncOracleCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Port")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)

    async def test_with_sid_only(self):
        mgr = AsyncOracleCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        creds = _sample()
        creds.pop("Service")
        creds["SID"] = "ORCLSID"
        with patch.object(mgr, "_publish_new_password", fake_publish):
            await mgr.new_credentials(creds)
        assert fake_publish.payload["GenericField3"] is None
        assert fake_publish.payload["GenericField4"] == "ORCLSID"
