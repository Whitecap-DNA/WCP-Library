"""Mock tests for AsyncCredentialManager in
``wcp_library/credentials/_credential_manager_asynchronous.py``.

Uses the concrete ``AsyncAPICredentialManager`` to exercise the abstract base.
All ``aiohttp`` calls are patched with fake async context managers — no real
network I/O.
"""
from unittest.mock import MagicMock, patch

import aiohttp
import pytest

from wcp_library.credentials import MissingCredentialsError
from wcp_library.credentials._credential_manager_asynchronous import (
    AsyncCredentialManager,
)
from wcp_library.credentials.api import AsyncAPICredentialManager


MODULE = "wcp_library.credentials._credential_manager_asynchronous"


class _FakeResponse:
    def __init__(self, *, json_value=None, status=200, raise_for_status_exc=None,
                 json_exc=None):
        self._json_value = json_value
        self.status = status
        self._raise_exc = raise_for_status_exc
        self._json_exc = json_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc

    async def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._json_value

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    """Fake aiohttp.ClientSession that returns preconfigured responses per verb."""

    def __init__(self, *, get_response=None, post_response=None,
                 put_response=None, get_exc=None, post_exc=None, put_exc=None):
        self._get_resp = get_response
        self._post_resp = post_response
        self._put_resp = put_response
        self._get_exc = get_exc
        self._post_exc = post_exc
        self._put_exc = put_exc
        self.post_kwargs = None
        self.put_kwargs = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *args, **kwargs):
        if self._get_exc:
            raise self._get_exc
        return self._get_resp

    def post(self, *args, **kwargs):
        self.post_kwargs = kwargs
        if self._post_exc:
            raise self._post_exc
        return self._post_resp

    def put(self, *args, **kwargs):
        self.put_kwargs = kwargs
        if self._put_exc:
            raise self._put_exc
        return self._put_resp


def _session_factory(**kwargs):
    """Return a no-arg callable aiohttp.ClientSession replacement."""
    session = _FakeSession(**kwargs)

    def factory(*_args, **_kwargs):
        return session

    factory.session = session
    return factory


def _entry(username="alice", password_id=11, password="pw",
           fields=None, otp=None, url=None):
    e = {
        "PasswordID": password_id,
        "UserName": username,
        "Password": password,
        "GenericFieldInfo": fields or [],
        "OTP": otp,
    }
    if url is not None:
        e["URL"] = url
    return e


class TestAsyncCredentialManagerConstruction:
    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            AsyncCredentialManager("k", 1)

    def test_concrete_subclass_attributes(self):
        mgr = AsyncAPICredentialManager("secret-key")
        assert mgr.api_key == "secret-key"
        assert mgr._password_list_id == 214
        assert mgr.headers["APIKey"] == "secret-key"
        assert str(mgr.password_url) == "https://vault.wcap.ca/api/passwords/"


class TestAsyncGetCredentials:
    async def test_happy_path(self):
        entry = _entry(
            username="Alice",
            fields=[
                {"DisplayName": "Host", "Value": "h.example",
                 "GenericFieldID": 5},
                {"DisplayName": "UserName", "Value": "INNER",
                 "GenericFieldID": 6},
            ],
            otp="tok",
            url="https://u",
        )
        resp = _FakeResponse(json_value=[entry])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            out = await mgr._get_credentials()
        assert "alice" in out
        info = out["alice"]
        assert info["Host"] == "h.example"
        assert info["UserName"] == "inner"  # lowercased via display-name rule
        assert info["OTP"] == "tok"
        assert info["URL"] == "https://u"

    async def test_empty_raises(self):
        resp = _FakeResponse(json_value=[])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError):
                await AsyncAPICredentialManager("k")._get_credentials()

    async def test_client_error_raises_missing(self):
        factory = _session_factory(get_exc=aiohttp.ClientError("boom"))
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError):
                await AsyncAPICredentialManager("k")._get_credentials()

    async def test_value_error_raises_missing(self):
        resp = _FakeResponse(json_exc=ValueError("bad json"))
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                await AsyncAPICredentialManager("k")._get_credentials()


class TestAsyncGetCredentialsPublic:
    async def test_lookup_by_username(self):
        entry = _entry(username="Bob", fields=[], otp=None)
        resp = _FakeResponse(json_value=[entry])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            cred = await mgr.get_credentials("BOB")
        assert cred["UserName"] == "Bob"

    async def test_missing_username_raises(self):
        entry = _entry(username="Bob", fields=[], otp=None)
        resp = _FakeResponse(json_value=[entry])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            with pytest.raises(MissingCredentialsError):
                await mgr.get_credentials("ghost")


class TestAsyncGetCredentialFromId:
    async def test_happy_path(self):
        entry = _entry(
            username="Alice", password_id=42,
            fields=[{"DisplayName": "Host", "Value": "h",
                     "GenericFieldID": 1}],
            otp="o", url="https://u",
        )
        resp = _FakeResponse(json_value=[entry])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            info = await AsyncAPICredentialManager("k").get_credential_from_id(42)
        assert info["PasswordID"] == 42
        assert info["Host"] == "h"
        assert info["OTP"] == "o"
        assert info["URL"] == "https://u"

    async def test_empty_raises(self):
        resp = _FakeResponse(json_value=[])
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError):
                await AsyncAPICredentialManager("k").get_credential_from_id(5)

    async def test_client_error_raises(self):
        factory = _session_factory(get_exc=aiohttp.ClientError("x"))
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError):
                await AsyncAPICredentialManager("k").get_credential_from_id(5)

    async def test_value_error_raises(self):
        resp = _FakeResponse(json_exc=ValueError("bad"))
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                await AsyncAPICredentialManager("k").get_credential_from_id(5)


class TestAsyncPublishNewPassword:
    async def test_201_returns_true(self):
        resp = _FakeResponse(status=201)
        factory = _session_factory(post_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            ok = await mgr._publish_new_password({"UserName": "u"})
        assert ok is True
        assert factory.session.post_kwargs["json"] == {"UserName": "u"}

    async def test_non_201_returns_false(self):
        resp = _FakeResponse(status=500)
        factory = _session_factory(post_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            ok = await mgr._publish_new_password({"UserName": "u"})
        assert ok is False

    async def test_client_error_returns_false(self):
        factory = _session_factory(post_exc=aiohttp.ClientError("boom"))
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            ok = await mgr._publish_new_password({"UserName": "u"})
        assert ok is False


class TestAsyncUpdateCredential:
    def _existing(self, username="alice"):
        return [{
            "PasswordID": 1,
            "UserName": username,
            "Password": "pw",
            "GenericFieldInfo": [
                {"DisplayName": "Host", "Value": "h",
                 "GenericFieldID": 77},
            ],
            "OTP": None,
        }]

    async def test_success(self):
        get_resp = _FakeResponse(json_value=self._existing())
        put_resp = _FakeResponse(status=200)
        # Each ClientSession() context needs its own session. Use two.
        sessions = [
            _FakeSession(get_response=get_resp),
            _FakeSession(put_response=put_resp),
        ]
        it = iter(sessions)
        with patch(f"{MODULE}.aiohttp.ClientSession",
                   lambda *a, **k: next(it)):
            mgr = AsyncAPICredentialManager("k")
            cred = {"UserName": "alice", "Password": "new", "Host": "h2",
                    "OTP": "pop"}
            ok = await mgr.update_credential(cred)
        assert ok is True
        sent = sessions[1].put_kwargs["json"]
        assert "OTP" not in sent
        assert 77 in sent
        assert sent[77] == "h2"
        assert "Host" not in sent

    async def test_non_200_returns_false(self):
        get_resp = _FakeResponse(json_value=self._existing())
        put_resp = _FakeResponse(status=500)
        sessions = [
            _FakeSession(get_response=get_resp),
            _FakeSession(put_response=put_resp),
        ]
        it = iter(sessions)
        with patch(f"{MODULE}.aiohttp.ClientSession",
                   lambda *a, **k: next(it)):
            mgr = AsyncAPICredentialManager("k")
            ok = await mgr.update_credential(
                {"UserName": "alice", "Password": "new"})
        assert ok is False

    async def test_get_client_error_raises(self):
        factory = _session_factory(get_exc=aiohttp.ClientError("x"))
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            with pytest.raises(MissingCredentialsError):
                await mgr.update_credential({"UserName": "alice"})

    async def test_get_value_error_raises(self):
        resp = _FakeResponse(json_exc=ValueError("bad"))
        factory = _session_factory(get_response=resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            with pytest.raises(MissingCredentialsError, match="Invalid JSON"):
                await mgr.update_credential({"UserName": "alice"})

    async def test_username_not_found_raises(self):
        # get returns passwords but none match the UserName
        get_resp = _FakeResponse(json_value=self._existing(username="someoneelse"))
        factory = _session_factory(get_response=get_resp)
        with patch(f"{MODULE}.aiohttp.ClientSession", factory):
            mgr = AsyncAPICredentialManager("k")
            with pytest.raises(MissingCredentialsError, match="not found"):
                await mgr.update_credential({"UserName": "alice"})

    async def test_put_client_error_returns_false(self):
        get_resp = _FakeResponse(json_value=self._existing())
        sessions = [
            _FakeSession(get_response=get_resp),
            _FakeSession(put_exc=aiohttp.ClientError("boom")),
        ]
        it = iter(sessions)
        with patch(f"{MODULE}.aiohttp.ClientSession",
                   lambda *a, **k: next(it)):
            mgr = AsyncAPICredentialManager("k")
            ok = await mgr.update_credential(
                {"UserName": "alice", "Password": "new"})
        assert ok is False


class TestAsyncNewCredentialsAbstract:
    def test_subclass_without_override_is_abstract(self):
        class Incomplete(AsyncCredentialManager):
            pass

        with pytest.raises(TypeError):
            Incomplete("k", 1)
