"""Mock tests for wcp_library.credentials.postgres."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.postgres import (
    AsyncPostgresCredentialManager,
    PostgresCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "PgUser",
        "Password": "pgpw",
        "Host": "pg.example.com",
        "Port": 5432,
        "Database": "mydb",
    }
    base.update(overrides)
    return base


class TestPostgresConstruction:
    def test_password_list_id_is_210(self):
        mgr = PostgresCredentialManager("k")
        assert mgr._password_list_id == 210


class TestPostgresNewCredentials:
    def test_happy_path_payload(self):
        mgr = PostgresCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 210
        assert p["Title"] == "PGUSER"
        assert p["UserName"] == "pguser"
        assert p["Password"] == "pgpw"
        assert p["GenericField1"] == "pg.example.com"
        assert p["GenericField2"] == 5432
        assert p["GenericField3"] == "mydb"
        assert p["Notes"] is None

    def test_title_override(self):
        mgr = PostgresCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="prod-pg"))
        assert mock_pub.call_args.args[0]["Title"] == "PROD-PG"

    def test_notes_included(self):
        mgr = PostgresCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Notes="primary db"))
        assert mock_pub.call_args.args[0]["Notes"] == "primary db"

    def test_missing_database_raises(self):
        mgr = PostgresCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("Database")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_false_propagates(self):
        mgr = PostgresCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncPostgresConstruction:
    def test_password_list_id_is_210(self):
        assert AsyncPostgresCredentialManager("k")._password_list_id == 210


class TestAsyncPostgresNewCredentials:
    async def test_happy_path(self):
        mgr = AsyncPostgresCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["PasswordListID"] == 210
        assert p["UserName"] == "pguser"
        assert p["Title"] == "PGUSER"
        assert p["GenericField3"] == "mydb"

    async def test_missing_host_raises(self):
        mgr = AsyncPostgresCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Host")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)
