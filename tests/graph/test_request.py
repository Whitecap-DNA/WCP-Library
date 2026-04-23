"""Mock tests for the shared _request helper in wcp_library.graph."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from wcp_library.graph import _request
from wcp_library.retry import _GraphRetriable


def _ok_response(status=200, payload=b""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.content = payload
    resp.headers = {}
    resp.raise_for_status.return_value = None
    return resp


class TestRequestHappyPath:
    def test_returns_response_on_2xx(self):
        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = _ok_response(200)
            result = _request("GET", "https://example.com", {"X": "1"})
            assert result.status_code == 200
            mock_req.assert_called_once()


class TestRequestRetries:
    def test_retries_on_429_then_succeeds(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep") as mock_sleep:
            bad = _ok_response(429)
            bad.headers = {"Retry-After": "1"}
            good = _ok_response(200)
            mock_req.side_effect = [bad, good]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200
            assert mock_req.call_count == 2
            mock_sleep.assert_called_once_with(1.0)

    def test_retries_on_503(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            bad = _ok_response(503)
            good = _ok_response(200)
            mock_req.side_effect = [bad, good]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200
            assert mock_req.call_count == 2

    def test_retries_on_connection_error(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            mock_req.side_effect = [requests.ConnectionError("boom"), _ok_response(200)]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200

    def test_gives_up_after_5_attempts(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            mock_req.return_value = _ok_response(503)
            with pytest.raises(_GraphRetriable):
                _request("GET", "https://example.com", {})
            assert mock_req.call_count == 5


class TestRequestNonRetryable:
    def test_500_raises_http_error_no_retry(self):
        resp = _ok_response(500)
        resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep") as mock_sleep:
            mock_req.return_value = resp
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", {})
            assert mock_req.call_count == 1
            mock_sleep.assert_not_called()

    def test_404_raises_http_error_no_retry(self):
        resp = _ok_response(404)
        resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = resp
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", {})


class TestRequestTimeoutConfig:
    def test_default_is_30_seconds(self, monkeypatch):
        """_request passes REQUEST_TIMEOUT through to requests.request."""
        import wcp_library.graph as graph_mod
        monkeypatch.setattr(graph_mod, "REQUEST_TIMEOUT", 30)

        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = _ok_response(200)
            _request("GET", "https://example.com", {})

        # Verify timeout= kwarg matches module-level REQUEST_TIMEOUT.
        assert mock_req.call_args.kwargs["timeout"] == 30

    def test_set_request_timeout_affects_next_call(self, monkeypatch):
        import wcp_library.graph as graph_mod
        monkeypatch.setattr(graph_mod, "REQUEST_TIMEOUT", 30)

        graph_mod.set_request_timeout(120)
        assert graph_mod.REQUEST_TIMEOUT == 120

        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = _ok_response(200)
            _request("GET", "https://example.com", {})

        assert mock_req.call_args.kwargs["timeout"] == 120

    def test_set_request_timeout_rejects_zero(self):
        from wcp_library.graph import set_request_timeout
        with pytest.raises(ValueError, match="must be positive"):
            set_request_timeout(0)

    def test_set_request_timeout_rejects_negative(self):
        from wcp_library.graph import set_request_timeout
        with pytest.raises(ValueError, match="must be positive"):
            set_request_timeout(-5)

    def test_set_request_timeout_accepts_float(self, monkeypatch):
        import wcp_library.graph as graph_mod
        monkeypatch.setattr(graph_mod, "REQUEST_TIMEOUT", 30)
        graph_mod.set_request_timeout(15.5)
        assert graph_mod.REQUEST_TIMEOUT == 15.5


class TestGraphRetriableContract:
    """Regression tests for the None-on-error contract of public graph
    helpers when tenacity exhausts its retry budget.

    Bug fixed in fix/graph-retriable-inheritance: _GraphRetriable used
    to inherit from Exception, so when `_request` exhausted its 5-attempt
    budget on a persistent 503, the exception escaped the public helper's
    `except requests.RequestException` clause and propagated to callers,
    violating the documented None-on-error contract.
    """

    def test_graph_retriable_is_request_exception(self):
        """_GraphRetriable must inherit from RequestException so that
        `except requests.RequestException` in every public helper catches
        it when retries are exhausted."""
        assert issubclass(_GraphRetriable, requests.RequestException)

    def test_graph_retriable_has_response_attribute(self):
        """Preserves the .response attribute for error inspection."""
        resp = _ok_response(503)
        exc = _GraphRetriable(response=resp)
        assert exc.response is resp

    def test_graph_retriable_has_underlying_attribute(self):
        """Preserves the .underlying attribute for network error cases."""
        net_err = requests.ConnectionError("boom")
        exc = _GraphRetriable(underlying=net_err)
        assert exc.underlying is net_err

    def test_sharepoint_helper_returns_none_when_retries_exhausted(self):
        """End-to-end: a public sharepoint helper returns None (not raises)
        when _request exhausts its retry budget on a persistent 503."""
        from wcp_library.graph import sharepoint

        persistent_503 = _ok_response(503)
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            mock_req.return_value = persistent_503
            result = sharepoint.get_site_metadata({}, "https://contoso.sharepoint.com/sites/x")

        assert result is None
        # Confirm retries were exhausted (5 attempts = 1 + 4 retries)
        assert mock_req.call_count == 5

    def test_mail_helper_returns_none_when_retries_exhausted(self):
        from wcp_library.graph import mail

        persistent_503 = _ok_response(503)
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            mock_req.return_value = persistent_503
            result = mail.get_mailbox_folders({}, "user@example.com")

        # `get_mailbox_folders` returns [] on error per its contract
        assert result == []
        assert mock_req.call_count == 5

    def test_sharepoint_helper_returns_none_on_persistent_connection_error(self):
        """Same path for network errors — _GraphRetriable(underlying=...)
        must be caught by the public helper's except clause."""
        from wcp_library.graph import sharepoint

        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("time.sleep"):
            mock_req.side_effect = requests.ConnectionError("network down")
            result = sharepoint.get_site_metadata({}, "https://contoso.sharepoint.com/sites/x")

        assert result is None
        assert mock_req.call_count == 5
