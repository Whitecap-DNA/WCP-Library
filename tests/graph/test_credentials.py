"""Mock tests for GraphCredentials and the token-expiry retry in _request."""
import threading
from unittest.mock import MagicMock, patch

import pytest
import requests

from wcp_library.graph import (GraphAuthError, GraphCredentials, _request,
                               sharepoint)
from wcp_library.retry import _GraphRetriable, make_generic_retry

# Mint retry without the real backoff, so mint-failure tests do not sleep.
_FAST_MINT_RETRY = make_generic_retry(
    GraphAuthError, max_attempts=3, delay=0, backoff=1, jitter=0
)


def _expired(status=401):
    """Build an HTTPError the way raise_for_status does, so .response is set."""
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} error", response=response)


def _graph_response(payload=None):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.json.return_value = payload if payload is not None else {}
    return resp


class _FakeVault:
    """Hands out a differently numbered token each call, so a re-mint is observable."""

    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return {"Authorization": f"Bearer token-{self.calls}"}


class _BlockingVault(_FakeVault):
    """A vault whose nth mint parks until released, to force an interleaving."""

    def __init__(self, block_on_call=2):
        super().__init__()
        self._block_on = block_on_call
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self):
        self.calls += 1
        if self.calls == self._block_on:
            self.entered.set()
            assert self.release.wait(timeout=5), "mint was never released"
        return {"Authorization": f"Bearer token-{self.calls}"}


class TestConstruction:
    def test_mints_exactly_one_token(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        assert vault.calls == 1
        assert credentials.generation == 1
        assert credentials.headers == {"Authorization": "Bearer token-1"}

    def test_from_vault_reads_the_vault_on_every_mint(self):
        with patch("wcp_library.graph.get_headers_from_vault") as mock_vault:
            mock_vault.return_value = {"Authorization": "Bearer from-vault"}
            credentials = GraphCredentials.from_vault("api-key", 4321)
            assert credentials.headers["Authorization"] == "Bearer from-vault"
            mock_vault.assert_called_once_with("api-key", 4321)

            # A rotated secret is picked up because the vault is re-read.
            mock_vault.return_value = {"Authorization": "Bearer rotated"}
            credentials.refresh(credentials.generation)
            assert credentials.headers["Authorization"] == "Bearer rotated"
            assert mock_vault.call_count == 2

    def test_from_app_registration_does_not_touch_the_vault(self):
        with patch("wcp_library.graph.get_headers") as mock_headers, \
             patch("wcp_library.graph.get_headers_from_vault") as mock_vault:
            mock_headers.return_value = {"Authorization": "Bearer direct"}
            credentials = GraphCredentials.from_app_registration(
                "app-id", "secret", "tenant-id"
            )
            assert credentials.headers["Authorization"] == "Bearer direct"
            mock_headers.assert_called_once_with("app-id", "secret", "tenant-id")
            mock_vault.assert_not_called()


class TestCall:
    def test_success_runs_the_operation_once(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        operation = MagicMock(return_value="result")

        assert credentials.call(operation) == "result"
        operation.assert_called_once_with(credentials.headers)
        assert vault.calls == 1

    def test_expiry_runs_the_operation_twice_and_returns_the_retry(self):
        credentials = GraphCredentials(_FakeVault())
        operation = MagicMock(side_effect=[_expired(), "second"])

        assert credentials.call(operation) == "second"
        assert operation.call_count == 2

    def test_the_retry_uses_the_new_token(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        seen = []

        def operation(headers):
            seen.append(headers["Authorization"])
            if len(seen) == 1:
                raise _expired()
            return "ok"

        assert credentials.call(operation) == "ok"
        assert seen == ["Bearer token-1", "Bearer token-2"]
        assert vault.calls == 2

    def test_a_remint_updates_the_dictionary_in_place(self):
        credentials = GraphCredentials(_FakeVault())
        captured = credentials.headers

        credentials.refresh(credentials.generation)

        assert captured is credentials.headers
        assert captured["Authorization"] == "Bearer token-2"

    @pytest.mark.parametrize("status", [403, 404, 500])
    def test_other_statuses_propagate_without_minting(self, status):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        operation = MagicMock(side_effect=_expired(status))

        with pytest.raises(requests.HTTPError):
            credentials.call(operation)

        operation.assert_called_once()
        assert vault.calls == 1
        assert credentials.generation == 1

    def test_http_error_without_a_response_propagates_without_minting(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        operation = MagicMock(side_effect=requests.HTTPError("no response attached"))

        with pytest.raises(requests.HTTPError):
            credentials.call(operation)

        operation.assert_called_once()
        assert vault.calls == 1

    def test_throttling_propagates_untouched(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        operation = MagicMock(side_effect=_GraphRetriable(underlying=OSError("down")))

        with pytest.raises(_GraphRetriable):
            credentials.call(operation)

        operation.assert_called_once()
        assert vault.calls == 1

    def test_persistent_expiry_raises_after_exactly_two_attempts(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        operation = MagicMock(side_effect=_expired())

        with pytest.raises(requests.HTTPError):
            credentials.call(operation)

        assert operation.call_count == 2
        assert vault.calls == 2


class TestRequestDictPath:
    """A plain dictionary must behave exactly as it did before refresh existed."""

    def test_401_propagates_and_is_not_retried(self):
        def always_expired(*args, **kwargs):
            raise _expired()

        with patch("wcp_library.graph._send", side_effect=always_expired) as mock_send:
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", {"Authorization": "Bearer t"})
            mock_send.assert_called_once()

    def test_success_passes_the_dictionary_straight_through(self):
        headers = {"Authorization": "Bearer t"}
        with patch("wcp_library.graph._send", return_value=_graph_response()) as mock_send:
            _request("GET", "https://example.com", headers)
            mock_send.assert_called_once_with("GET", "https://example.com", headers)


class TestRequestCredentialsPath:
    def test_401_remints_and_retries_the_request(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        seen = []

        def send(method, url, headers, **kwargs):
            seen.append(headers["Authorization"])
            if headers["Authorization"] == "Bearer token-1":
                raise _expired()
            return _graph_response({"ok": True})

        with patch("wcp_library.graph._send", side_effect=send):
            response = _request("GET", "https://example.com", credentials)

        assert response.json() == {"ok": True}
        assert seen == ["Bearer token-1", "Bearer token-2"]
        assert vault.calls == 2

    def test_403_is_not_retried(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)

        def forbidden(*args, **kwargs):
            raise _expired(403)

        with patch("wcp_library.graph._send", side_effect=forbidden) as mock_send:
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", credentials)

        mock_send.assert_called_once()
        assert vault.calls == 1

    def test_persistent_401_raises_after_exactly_two_requests(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)

        def always_expired(*args, **kwargs):
            raise _expired()

        with patch("wcp_library.graph._send", side_effect=always_expired) as mock_send:
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", credentials)

        assert mock_send.call_count == 2
        assert vault.calls == 2


class TestRequestExtraHeaders:
    """Helpers that add a Content-Type must not collapse credentials to a dict."""

    def test_credentials_survive_a_call_that_adds_headers(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)
        seen = []

        def send(method, url, headers, **kwargs):
            seen.append(dict(headers))
            if headers["Authorization"] == "Bearer token-1":
                raise _expired()
            return _graph_response({"id": "1"})

        with patch("wcp_library.graph._send", side_effect=send):
            result = sharepoint.create_list_item(
                credentials, "site-id", "list-id", {"Title": "x"}
            )

        assert result == {"id": "1"}
        assert [h["Authorization"] for h in seen] == ["Bearer token-1", "Bearer token-2"]
        # The per-call Content-Type is still there after the re-mint.
        assert all(h["Content-Type"] == "application/json" for h in seen)
        assert vault.calls == 2

    def test_a_dict_caller_still_gets_the_added_header(self):
        headers = {"Authorization": "Bearer t"}
        with patch(
            "wcp_library.graph._send", return_value=_graph_response({})
        ) as mock_send:
            sharepoint.create_list_item(headers, "site-id", "list-id", {"Title": "x"})
            sent = mock_send.call_args[0][2]

        assert sent == {"Authorization": "Bearer t", "Content-Type": "application/json"}
        # The dictionary the caller handed in is left alone.
        assert headers == {"Authorization": "Bearer t"}


class TestConcurrency:
    def test_concurrent_expiry_produces_one_remint(self):
        vault = _BlockingVault()
        credentials = GraphCredentials(vault)
        seen_generation = credentials.generation
        results = []

        def refresh():
            results.append(credentials.refresh(seen_generation))

        threads = [threading.Thread(target=refresh) for _ in range(5)]
        for thread in threads:
            thread.start()

        assert vault.entered.wait(timeout=5), "no thread reached the mint"
        vault.release.set()
        for thread in threads:
            thread.join(timeout=5)

        # Five racers that all saw token-1 share a single re-mint between them.
        assert vault.calls == 2
        assert results == [2, 2, 2, 2, 2]
        assert credentials.headers["Authorization"] == "Bearer token-2"

    def test_a_reader_during_an_inflight_mint_sees_a_usable_token(self):
        vault = _BlockingVault()
        credentials = GraphCredentials(vault)
        minter = threading.Thread(target=credentials.refresh, args=(1,))
        minter.start()

        assert vault.entered.wait(timeout=5), "the mint never started"
        # The mint is in flight and the lock is held. A reader must still find a
        # token here: clearing the dictionary first would raise KeyError.
        observed = credentials.headers["Authorization"]

        vault.release.set()
        minter.join(timeout=5)

        assert observed == "Bearer token-1"
        assert credentials.headers["Authorization"] == "Bearer token-2"


class TestMintFailure:
    def test_a_transient_mint_failure_is_retried(self):
        source = MagicMock(
            side_effect=[
                {"Authorization": "Bearer first"},
                GraphAuthError("vault blipped"),
                {"Authorization": "Bearer second"},
            ]
        )
        with patch("wcp_library.graph._mint_retry_kwargs", _FAST_MINT_RETRY):
            credentials = GraphCredentials(source)
            credentials.refresh(credentials.generation)

        assert credentials.headers["Authorization"] == "Bearer second"
        assert source.call_count == 3

    def test_a_persistent_mint_failure_raises_graph_auth_error(self):
        source = MagicMock(
            side_effect=[
                {"Authorization": "Bearer first"},
                GraphAuthError("token endpoint down"),
                GraphAuthError("token endpoint down"),
                GraphAuthError("token endpoint down"),
            ]
        )
        with patch("wcp_library.graph._mint_retry_kwargs", _FAST_MINT_RETRY):
            credentials = GraphCredentials(source)
            with pytest.raises(GraphAuthError):
                credentials.refresh(credentials.generation)

        # The old token is still in place, and it is a GraphAuthError that
        # surfaced, not a requests exception.
        assert credentials.headers["Authorization"] == "Bearer first"
        assert credentials.generation == 1


class TestUploadMultipleFilesExpiry:
    """The multi-file path spawns a thread per file over shared credentials."""

    def test_an_expired_token_is_reminted_not_reported_as_a_failure(self):
        vault = _FakeVault()
        credentials = GraphCredentials(vault)

        def send(method, url, headers, **kwargs):
            if headers["Authorization"] == "Bearer token-1":
                raise _expired()
            return _graph_response({"webUrl": url})

        with patch("wcp_library.graph._send", side_effect=send):
            results = sharepoint.upload_multiple_files(
                credentials,
                "site-id",
                [("/Shared", "a.txt", b"a"), ("/Shared", "b.txt", b"b")],
            )

        # One re-mint shared by both threads, and both files land.
        assert vault.calls == 2
        assert len(results) == 2
        assert "a.txt" in results[0]["webUrl"]
        assert "b.txt" in results[1]["webUrl"]

    def test_failures_raise_an_exception_group_naming_each_file(self):
        def always_fails(*args, **kwargs):
            raise _expired(500)

        with patch("wcp_library.graph._send", side_effect=always_fails):
            with pytest.raises(ExceptionGroup) as exc_info:
                sharepoint.upload_multiple_files(
                    {"Authorization": "Bearer t"},
                    "site-id",
                    [("/Shared", "a.txt", b"a"), ("/Shared", "b.txt", b"b")],
                )

        group = exc_info.value
        assert len(group.exceptions) == 2
        notes = [note for error in group.exceptions for note in error.__notes__]
        assert "file: /Shared/a.txt" in notes
        assert "file: /Shared/b.txt" in notes

    def test_a_fully_successful_batch_returns_responses_in_order(self):
        def send(method, url, headers, **kwargs):
            return _graph_response({"webUrl": url})

        with patch("wcp_library.graph._send", side_effect=send):
            results = sharepoint.upload_multiple_files(
                {"Authorization": "Bearer t"},
                "site-id",
                [("/Shared", "a.txt", b"a"), ("/Shared", "b.txt", b"b"),
                 ("/Shared", "c.txt", b"c")],
            )

        assert len(results) == 3
        for expected, result in zip(("a.txt", "b.txt", "c.txt"), results):
            assert expected in result["webUrl"]
