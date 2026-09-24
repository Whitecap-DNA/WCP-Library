"""Mock tests for wcp_library.graph.subscription.

All HTTP calls are patched via unittest.mock. No network access occurs.
"""
from datetime import datetime, timedelta, timezone
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
            "wcp_library.graph._request",
            return_value=_ok_json(payload),
        ) as mock_request:
            result = subscription.list_subscriptions(HEADERS)
            assert result == [{"id": "s1"}, {"id": "s2"}]
            called_url = mock_request.call_args[0][1]
            assert called_url == "https://graph.microsoft.com/v1.0/subscriptions"

    def test_returns_empty_list_when_value_missing(self):
        with patch(
            "wcp_library.graph._request",
            return_value=_ok_json({}),
        ):
            assert subscription.list_subscriptions(HEADERS) == []

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                subscription.list_subscriptions(HEADERS)


# ======================= renew_expiring_subscriptions ======================= #


class TestRenewExpiringSubscriptions:
    def test_renews_subscriptions_expiring_within_threshold(self):
        soon = datetime.now(timezone.utc) + timedelta(minutes=5)
        far = datetime.now(timezone.utc) + timedelta(days=10)
        subscriptions = [
            {
                "id": "expiring",
                "expirationDateTime": soon.isoformat().replace("+00:00", "Z"),
            },
            {
                "id": "not-expiring",
                "expirationDateTime": far.isoformat().replace("+00:00", "Z"),
            },
        ]
        with patch(
            "wcp_library.graph.subscription.list_subscriptions",
            return_value=subscriptions,
        ), patch(
            "wcp_library.graph.subscription.update_subscription_expiration",
            return_value={"id": "expiring", "renewed": True},
        ) as mock_update:
            renewed = subscription.renew_expiring_subscriptions(
                HEADERS, threshold_minutes=60
            )
            assert renewed == [{"id": "expiring", "renewed": True}]
            mock_update.assert_called_once_with(HEADERS, "expiring")

    def test_skips_subscriptions_missing_id_or_expiration(self):
        subscriptions = [
            {"expirationDateTime": "2020-01-01T00:00:00Z"},  # no id
            {"id": "no-expiration"},  # no expirationDateTime
        ]
        with patch(
            "wcp_library.graph.subscription.list_subscriptions",
            return_value=subscriptions,
        ), patch(
            "wcp_library.graph.subscription.update_subscription_expiration"
        ) as mock_update:
            renewed = subscription.renew_expiring_subscriptions(HEADERS)
            assert renewed == []
            mock_update.assert_not_called()

    def test_one_renewal_failure_does_not_stop_the_sweep(self):
        soon = datetime.now(timezone.utc) + timedelta(minutes=5)
        expiring_soon = soon.isoformat().replace("+00:00", "Z")
        subscriptions = [
            {"id": "fails", "expirationDateTime": expiring_soon},
            {"id": "succeeds", "expirationDateTime": expiring_soon},
        ]

        def _fake_update(headers, subscription_id):
            if subscription_id == "fails":
                raise _http_error()
            return {"id": subscription_id, "renewed": True}

        with patch(
            "wcp_library.graph.subscription.list_subscriptions",
            return_value=subscriptions,
        ), patch(
            "wcp_library.graph.subscription.update_subscription_expiration",
            side_effect=_fake_update,
        ):
            renewed = subscription.renew_expiring_subscriptions(
                HEADERS, threshold_minutes=60
            )
            assert renewed == [{"id": "succeeds", "renewed": True}]

    def test_raises_when_listing_subscriptions_fails(self):
        with patch(
            "wcp_library.graph.subscription.list_subscriptions",
            side_effect=_http_error(),
        ):
            with pytest.raises(requests.RequestException):
                subscription.renew_expiring_subscriptions(HEADERS)


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


# ======================= get_resource_type ======================= #


class TestGetResourceType:
    @pytest.mark.parametrize(
        "resource, expected",
        [
            ("users/user@example.com/messages", "mail"),
            ("users/user@example.com/events", "calendar"),
            ("users/user@example.com/contacts", "contacts"),
            ("me/drive/root", "drive"),
            ("sites/contoso.sharepoint.com,abc,def/lists/x", "sharepoint"),
            ("groups/abc-123", "directory"),
            ("users/abc-123", "directory"),
            ("communications/presences/abc", "presence"),
            ("print/printers/abc/jobs", "print"),
            ("security/alerts_v2", "security"),
            ("something/unrecognized", "default"),
        ],
    )
    def test_maps_resource_path_to_type(self, resource, expected):
        assert subscription.get_resource_type(resource) == expected

    def test_matching_is_case_insensitive(self):
        assert subscription.get_resource_type("SITES/x/lists/y") == "sharepoint"

    # --- Known bugs in the mapping table ---
    #
    # Several resource shapes nest a specific collection under a generic
    # scope segment that is ALSO a mapping key, so the specific one has to
    # win. get_resource_type splits the path into segments and walks an
    # ordered priority list, most specific first, which resolves each of
    # these. They are kept as regression guards: reordering that list, or
    # going back to an unanchored substring match, silently reintroduces
    # the misclassification each one names.

    def test_teams_channel_message_is_teams(self):
        # "teams/{id}/channels/{id}/messages/{id}" has both "messages" and
        # "teams"; "teams" is the more specific match.
        assert (
            subscription.get_resource_type("teams/t1/channels/c1/messages/m1")
            == "teams"
        )

    def test_chat_message_is_teams(self):
        # Same collision for chat messages ("chats/{id}/messages/{id}"), and
        # "chats" maps onto the shared "teams" lifetime.
        assert subscription.get_resource_type("chats/c1/messages/m1") == "teams"

    def test_todo_task_is_todo(self):
        # "users/{id}/todo/lists/{id}/tasks/{id}" has both "users" and "todo".
        assert (
            subscription.get_resource_type("users/u1/todo/lists/l1/tasks/t1")
            == "todo"
        )

    def test_user_scoped_copilot_is_copilot(self):
        # "copilot/users/{id}/..." has both "users" and "copilot".
        assert (
            subscription.get_resource_type("copilot/users/u1/interactionhistory")
            == "copilot"
        )


# ======================= recreate_subscription ======================= #


class TestRecreateSubscription:
    # Graph hands back the notificationUrl that was submitted, and
    # create_subscription submits "<base>/api/graph" - so a realistic stored
    # value carries that suffix. Using the bare base here hid a bug where the
    # suffix was appended a second time.
    STORED_URL = f"{NOTIFICATION_URL}/api/graph"

    def _existing(self, **overrides):
        return {
            "id": SUBSCRIPTION_ID,
            "notificationUrl": self.STORED_URL,
            "resource": RESOURCE,
            "changeType": "created",
            "clientState": CLIENT_STATE,
        } | overrides

    def test_recreates_using_existing_subscription_values(self):
        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=self._existing(),
        ) as mock_get, patch(
            "wcp_library.graph.subscription.create_subscription"
        ) as mock_create:
            subscription.recreate_subscription(HEADERS, SUBSCRIPTION_ID)
            mock_get.assert_called_once_with(HEADERS, SUBSCRIPTION_ID)
            mock_create.assert_called_once_with(
                HEADERS,
                # The base URL, not the stored one: create_subscription
                # appends the notification path itself.
                NOTIFICATION_URL,
                "mail",  # get_resource_type maps "messages" in the path to "mail"
                RESOURCE,
                "created",
                CLIENT_STATE,
            )

    def test_recreated_url_matches_the_original_not_doubled(self):
        # End to end through the real create_subscription, so the URL the
        # payload actually carries is asserted rather than the argument
        # forwarded to a mock.
        sent = {}

        def fake_send(method, url, headers, **kwargs):
            sent.update(kwargs.get("json") or {})
            resp = MagicMock()
            resp.json.return_value = {"id": "new-sub"}
            return resp

        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=self._existing(),
        ), patch("wcp_library.graph._send", side_effect=fake_send):
            subscription.recreate_subscription(HEADERS, SUBSCRIPTION_ID)

        assert sent["notificationUrl"] == self.STORED_URL
        assert sent["lifecycleNotificationUrl"] == f"{NOTIFICATION_URL}/api/lifecycle"
        assert "/api/graph/api/" not in sent["notificationUrl"]

    @pytest.mark.parametrize(
        "field", ["notificationUrl", "resource", "changeType", "clientState"]
    )
    def test_missing_required_field_raises_valueerror(self, field):
        # These used to reach create_subscription as None, producing a
        # malformed payload, or crash inside get_resource_type.
        with patch(
            "wcp_library.graph.subscription.get_subscription",
            return_value=self._existing(**{field: None}),
        ):
            with pytest.raises(ValueError, match=field):
                subscription.recreate_subscription(HEADERS, SUBSCRIPTION_ID)


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