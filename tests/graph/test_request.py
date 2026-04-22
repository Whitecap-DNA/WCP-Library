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
