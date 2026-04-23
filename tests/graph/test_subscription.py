"""Mock tests for wcp_library.graph.subscription.

All HTTP calls are patched via unittest.mock. No network access occurs.
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from wcp_library.graph import subscription


# --------------------------- Helpers --------------------------- #


def _ok_json(payload, status_code=200):
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = payload
    mock.status_code = status_code
    return mock


def _ok_no_json(status_code=200):
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.status_code = status_code
    return mock


def _http_error(status=500):
    err = requests.exceptions.RequestException("boom")
    err.response = MagicMock(status_code=status, text="error body")
    return err


HEADERS = {"Authorization": "Bearer testtoken"}
SUBSCRIPTION_ID = "sub-abc-123"
NOTIFICATION_URL = "https://my-relay.example.com"
RESOURCE = "users/user@example.com/messages"
CLIENT_STATE = "secret-state"


# ======================= create_subscription ======================= #


class TestCreateSubscription:
    def test_posts_subscription_payload_with_derived_urls(self):
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_json({"id": SUBSCRIPTION_ID}),
        ) as mock_request:
            subscription.create_subscription(
                HEADERS,
                notification_url=NOTIFICATION_URL,
                resource_type="mail",
                resource=RESOURCE,
                change_type="created",
                client_state=CLIENT_STATE,
            )
            called_method = mock_request.call_args[0][0]
            called_url = mock_request.call_args[0][1]
            assert called_method == "POST"
            assert called_url == "https://graph.microsoft.com/v1.0/subscriptions"
            sent = mock_request.call_args.kwargs["json"]
            assert sent["changeType"] == "created"
            assert sent["clientState"] == CLIENT_STATE
            assert sent["resource"] == RESOURCE
            assert sent["notificationUrl"] == f"{NOTIFICATION_URL}/api/graph"
            assert (
                sent["lifecycleNotificationUrl"]
                == f"{NOTIFICATION_URL}/api/lifecycle"
            )
            assert "expirationDateTime" in sent
            # ISO timestamp, should end with Z
            assert sent["expirationDateTime"].endswith("Z")
            assert mock_request.call_args[0][2] == HEADERS

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request",
            side_effect=_http_error(),
        ):
            with pytest.raises(requests.RequestException):
                subscription.create_subscription(
                    HEADERS,
                    NOTIFICATION_URL,
                    "mail",
                    RESOURCE,
                    "created",
                    CLIENT_STATE,
                )


# ======================= get_subscription ======================= #


class TestGetSubscription:
    def test_returns_subscription_json(self):
        payload = {
            "id": SUBSCRIPTION_ID,
            "resource": RESOURCE,
            "changeType": "created",
        }
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_json(payload),
        ) as mock_request:
            result = subscription.get_subscription(HEADERS, SUBSCRIPTION_ID)
            assert result == payload
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
            )
            assert mock_request.call_args[0][2] == HEADERS

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.get_subscription(HEADERS, SUBSCRIPTION_ID)


# ======================= update_subscription_expiration ======================= #


class TestUpdateSubscriptionExpiration:
    def test_patches_with_new_expiration_datetime(self):
        existing = {
            "id": SUBSCRIPTION_ID,
            "resource": RESOURCE,
        }
        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=existing,
        ) as mock_get_sub, patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_no_json(),
        ) as mock_request:
            subscription.update_subscription_expiration(HEADERS, SUBSCRIPTION_ID)
            mock_get_sub.assert_called_once_with(HEADERS, SUBSCRIPTION_ID)
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
            )
            body = mock_request.call_args.kwargs["json"]
            assert set(body.keys()) == {"expirationDateTime"}
            assert body["expirationDateTime"].endswith("Z")

    def test_raises_on_request_exception(self):
        existing = {"id": SUBSCRIPTION_ID, "resource": RESOURCE}
        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=existing,
        ), patch(
            "wcp_library.graph.subscription._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.update_subscription_expiration(HEADERS, SUBSCRIPTION_ID)


# ======================= list_subscriptions ======================= #


class TestListSubscriptions:
    def test_returns_subscription_values(self):
        payload = {"value": [{"id": "s1"}, {"id": "s2"}]}
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_json(payload),
        ) as mock_request:
            result = subscription.list_subscriptions(HEADERS)
            assert result == [{"id": "s1"}, {"id": "s2"}]
            called_url = mock_request.call_args[0][1]
            assert called_url == "https://graph.microsoft.com/v1.0/subscriptions"

    def test_returns_empty_list_when_value_missing(self):
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_json({}),
        ):
            assert subscription.list_subscriptions(HEADERS) == []

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.list_subscriptions(HEADERS)


# ======================= delete_subscription ======================= #


class TestDeleteSubscription:
    def test_issues_delete_request_to_subscription_url(self):
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_no_json(),
        ) as mock_request:
            result = subscription.delete_subscription(HEADERS, SUBSCRIPTION_ID)
            assert result is None
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
            )
            assert mock_request.call_args[0][2] == HEADERS

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request",
            side_effect=_http_error(),
        ):
            with pytest.raises(requests.RequestException):
                subscription.delete_subscription(HEADERS, SUBSCRIPTION_ID)


# ======================= reauthorize_subscription ======================= #


class TestReauthorizeSubscription:
    def test_posts_to_reauthorize_endpoint(self):
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_no_json(),
        ) as mock_request:
            subscription.reauthorize_subscription(HEADERS, SUBSCRIPTION_ID)
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
                "/reauthorize"
            )
            assert mock_request.call_args[0][2] == HEADERS

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.reauthorize_subscription(HEADERS, SUBSCRIPTION_ID)


# ======================= recreate_subscription ======================= #


class TestRecreateSubscription:
    def test_recreates_using_existing_subscription_values(self):
        existing = {
            "id": SUBSCRIPTION_ID,
            "notificationUrl": NOTIFICATION_URL,
            "resource": RESOURCE,
            "changeType": "created",
            "clientState": CLIENT_STATE,
        }
        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=existing,
        ) as mock_get, patch(
            "wcp_library.graph.subscription.create_subscription"
        ) as mock_create:
            subscription.recreate_subscription(HEADERS, SUBSCRIPTION_ID)
            mock_get.assert_called_once_with(HEADERS, SUBSCRIPTION_ID)
            mock_create.assert_called_once_with(
                HEADERS,
                NOTIFICATION_URL,
                RESOURCE.split("/")[0],  # resource_type inferred as first segment
                RESOURCE,
                "created",
                CLIENT_STATE,
            )


# ======================= update_notification_url ======================= #


class TestUpdateNotificationUrl:
    def test_patches_subscription_with_new_url(self):
        new_url = "https://new-relay.example.com/hook"
        with patch(
            "wcp_library.graph.subscription._request",
            return_value=_ok_no_json(),
        ) as mock_request:
            subscription.update_notification_url(HEADERS, SUBSCRIPTION_ID, new_url)
            called_url = mock_request.call_args[0][1]
            assert called_url == (
                f"https://graph.microsoft.com/v1.0/subscriptions/{SUBSCRIPTION_ID}"
            )
            assert mock_request.call_args.kwargs["json"] == {"notificationUrl": new_url}
            assert mock_request.call_args[0][2] == HEADERS

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.subscription._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.update_notification_url(
                    HEADERS, SUBSCRIPTION_ID, "https://x"
                )
