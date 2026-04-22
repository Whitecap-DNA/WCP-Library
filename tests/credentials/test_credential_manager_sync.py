"""Mock tests for CredentialManager (synchronous) in
``wcp_library/credentials/_credential_manager_synchronous.py``.

``CredentialManager`` is abstract — we exercise it via the concrete
``APICredentialManager`` subclass. All ``requests`` calls are patched.
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from wcp_library.credentials import MissingCredentialsError
from wcp_library.credentials._credential_manager_synchronous import CredentialManager
from wcp_library.credentials.api import APICredentialManager


MODULE = "wcp_library.credentials._credential_manager_synchronous"


def _make_password_entry(username="alice", password_id=11, password="pw",
                         fields=None, otp=None, url=None):
    entry = {
        "PasswordID": password_id,
        "UserName": username,
        "Password": password,
        "GenericFieldInfo": fields or [],
        "OTP": otp,
    }
    if url is not None:
        entry["URL"] = url
    return entry


class TestCredentialManagerConstruction:
    def test_cannot_instantiate_abstract_base(self):
        with pytest.raises(TypeError):
            CredentialManager("key", 1)  # abstract, has abstractmethod

    def test_subclass_sets_expected_attributes(self):
        mgr = APICredentialManager("my-api-key")
        assert mgr.api_key == "my-api-key"
        assert mgr._password_list_id == 214
        assert mgr.headers["APIKey"] == "my-api-key"
        assert mgr.headers["Reason"] == "Python Script Access"
        assert str(mgr.password_url) == "https://vault.wcap.ca/api/passwords/"


class TestGetCredentials:
    def _mgr(self):
        return APICredentialManager("k")

    def test_happy_path_builds_dict(self):
        entry = _make_password_entry(
            username="Alice",
            password_id=1,
            password="pw1",
            fields=[
                {"DisplayName": "Host", "Value": "db.example.com",
                 "GenericFieldID": 10},
                {"DisplayName": "UserName", "Value": "OTHER_USER",
                 "GenericFieldID": 11},
            ],
            otp="otp-secret",
            url="https://example.com",
        )
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = [entry]
            out = self._mgr()._get_credentials()
        assert "alice" in out
        info = out["alice"]
        assert info["PasswordID"] == 1
        assert info["Password"] == "pw1"
        assert info["Host"] == "db.example.com"
        # UserName DisplayName values get lower-cased
        assert info["UserName"] == "other_user"
        assert info["URL"] == "https://example.com"
        assert info["OTP"] == "otp-secret"

    def test_empty_list_raises(self):
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = []
            with pytest.raises(MissingCredentialsError):
                self._mgr()._get_credentials()

    def test_timeout_raises(self):
        with patch(f"{MODULE}.requests.get", side_effect=requests.Timeout()):
            with pytest.raises(MissingCredentialsError, match="Timeout"):
                self._mgr()._get_credentials()

    def test_http_error_raises(self):
        http_err = requests.HTTPError(response=MagicMock(status_code=500))
        resp = MagicMock()
        resp.raise_for_status.side_effect = http_err
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="HTTP error"):
                self._mgr()._get_credentials()

    def test_value_error_raises(self):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.side_effect = ValueError("bad json")
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                self._mgr()._get_credentials()


class TestGetCredentialsPublic:
    def test_lookup_by_username(self):
        entry = _make_password_entry(username="Bob", fields=[], otp=None)
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = [entry]
            mgr = APICredentialManager("k")
            cred = mgr.get_credentials("BOB")
        assert cred["UserName"] == "Bob"

    def test_lookup_missing_raises(self):
        entry = _make_password_entry(username="Bob", fields=[], otp=None)
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = [entry]
            mgr = APICredentialManager("k")
            with pytest.raises(MissingCredentialsError,
                               match="not found in this Password List"):
                mgr.get_credentials("ghost")


class TestGetCredentialFromId:
    def _mgr(self):
        return APICredentialManager("k")

    def test_happy_path(self):
        entry = _make_password_entry(
            username="Alice", password_id=99,
            fields=[{"DisplayName": "Host", "Value": "h",
                     "GenericFieldID": 1}],
            otp="o", url="https://u",
        )
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = [entry]
            info = self._mgr().get_credential_from_id(99)
        assert info["PasswordID"] == 99
        assert info["Host"] == "h"
        assert info["OTP"] == "o"
        assert info["URL"] == "https://u"

    def test_empty_raises(self):
        with patch(f"{MODULE}.requests.get") as mock_get:
            mock_get.return_value.raise_for_status = MagicMock()
            mock_get.return_value.json.return_value = []
            with pytest.raises(MissingCredentialsError,
                               match="No credentials found"):
                self._mgr().get_credential_from_id(12)

    def test_timeout(self):
        with patch(f"{MODULE}.requests.get", side_effect=requests.Timeout()):
            with pytest.raises(MissingCredentialsError, match="Timeout"):
                self._mgr().get_credential_from_id(12)

    def test_http_error(self):
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=404))
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="HTTP error"):
                self._mgr().get_credential_from_id(12)

    def test_value_error(self):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.side_effect = ValueError("x")
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                self._mgr().get_credential_from_id(12)


class TestPublishNewPassword:
    def _mgr(self):
        return APICredentialManager("k")

    def test_success_returns_true(self):
        resp = MagicMock(status_code=201)
        data = {"UserName": "u"}
        with patch(f"{MODULE}.requests.post", return_value=resp) as mock_post:
            ok = self._mgr()._publish_new_password(data)
        assert ok is True
        mock_post.assert_called_once()

    def test_non_201_returns_false(self):
        resp = MagicMock(status_code=500)
        data = {"UserName": "u"}
        with patch(f"{MODULE}.requests.post", return_value=resp):
            assert self._mgr()._publish_new_password(data) is False

    def test_timeout_returns_false(self):
        data = {"UserName": "u"}
        with patch(f"{MODULE}.requests.post",
                   side_effect=requests.Timeout()):
            assert self._mgr()._publish_new_password(data) is False

    def test_request_exception_returns_false(self):
        data = {"UserName": "u"}
        with patch(f"{MODULE}.requests.post",
                   side_effect=requests.RequestException("boom")):
            assert self._mgr()._publish_new_password(data) is False


class TestUpdateCredential:
    def _make_existing_record(self, username="alice"):
        return {
            "PasswordID": 1,
            "UserName": username,
            "Password": "pw",
            "GenericFieldInfo": [
                {"DisplayName": "Host", "Value": "h",
                 "GenericFieldID": 77},
            ],
            "OTP": None,
        }

    def test_success_returns_true(self):
        mgr = APICredentialManager("k")
        get_resp = MagicMock()
        get_resp.raise_for_status = MagicMock()
        get_resp.json.return_value = [self._make_existing_record()]
        put_resp = MagicMock(status_code=200)
        cred = {"UserName": "alice", "Password": "new", "Host": "h2",
                "OTP": "should-be-popped"}
        with patch(f"{MODULE}.requests.get", return_value=get_resp), \
                patch(f"{MODULE}.requests.put", return_value=put_resp) as mp:
            ok = mgr.update_credential(cred)
        assert ok is True
        # OTP must have been stripped before PUT
        sent = mp.call_args.kwargs["json"]
        assert "OTP" not in sent
        # Host renamed to the GenericFieldID (77)
        assert 77 in sent
        assert sent[77] == "h2"
        assert "Host" not in sent

    def test_non_200_returns_false(self):
        mgr = APICredentialManager("k")
        get_resp = MagicMock()
        get_resp.raise_for_status = MagicMock()
        get_resp.json.return_value = [self._make_existing_record()]
        put_resp = MagicMock(status_code=500)
        cred = {"UserName": "alice", "Password": "new"}
        with patch(f"{MODULE}.requests.get", return_value=get_resp), \
                patch(f"{MODULE}.requests.put", return_value=put_resp):
            assert mgr.update_credential(cred) is False

    def test_timeout_during_get_raises(self):
        mgr = APICredentialManager("k")
        with patch(f"{MODULE}.requests.get", side_effect=requests.Timeout()):
            with pytest.raises(MissingCredentialsError, match="Timeout"):
                mgr.update_credential({"UserName": "alice"})

    def test_http_error_during_get_raises(self):
        mgr = APICredentialManager("k")
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=403))
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="HTTP error"):
                mgr.update_credential({"UserName": "alice"})

    def test_value_error_during_get_raises(self):
        mgr = APICredentialManager("k")
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.side_effect = ValueError("x")
        with patch(f"{MODULE}.requests.get", return_value=resp):
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                mgr.update_credential({"UserName": "alice"})


class TestNewCredentialsIsAbstract:
    """The abstract method on CredentialManager prevents direct instantiation.
    A subclass that does NOT override new_credentials should also fail."""

    def test_subclass_without_override_still_abstract(self):
        class Incomplete(CredentialManager):
            pass

        with pytest.raises(TypeError):
            Incomplete("k", 1)
