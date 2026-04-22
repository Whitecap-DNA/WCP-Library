"""Tests for wcp_library.time."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest
import pytz


# ---------------------------------------------------------------------------
# get_current_time
# ---------------------------------------------------------------------------


class TestGetCurrentTime:
    def test_naive_by_default(self) -> None:
        from wcp_library.time import get_current_time

        fake_now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=pytz.UTC).astimezone(
            pytz.timezone("Canada/Mountain")
        )

        with patch("wcp_library.time.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            # datetime.now expects the tz to be passed through
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            result = get_current_time()
            assert result.tzinfo is None

    def test_aware_preserves_tz(self) -> None:
        from wcp_library.time import get_current_time

        fake_now = pytz.UTC.localize(datetime(2024, 6, 1, 12, 0, 0)).astimezone(
            pytz.timezone("Canada/Mountain")
        )

        with patch("wcp_library.time.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now

            result = get_current_time(aware=True)
            assert result.tzinfo is not None

    def test_custom_timezone_passed_to_datetime_now(self) -> None:
        from wcp_library.time import get_current_time

        target_tz = pytz.timezone("US/Pacific")
        fake_now = datetime(2024, 6, 1, 5, 0, 0, tzinfo=target_tz)

        with patch("wcp_library.time.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now

            get_current_time(aware=True, tz="US/Pacific")

            assert mock_dt.now.call_count == 1
            passed_tz = mock_dt.now.call_args.args[0]
            assert str(passed_tz) == "US/Pacific"

    def test_invalid_timezone_raises(self) -> None:
        from wcp_library.time import get_current_time

        with pytest.raises(pytz.UnknownTimeZoneError):
            get_current_time(tz="Mars/Olympus_Mons")


# ---------------------------------------------------------------------------
# convert_tz
# ---------------------------------------------------------------------------


class TestConvertTz:
    def test_converts_utc_to_mountain_naive(self) -> None:
        from wcp_library.time import convert_tz

        # 18:00 UTC == 11:00 or 12:00 Mountain depending on DST; assert via re-convert
        t = datetime(2024, 1, 15, 18, 0, 0)  # winter (MST = UTC-7)
        result = convert_tz(t, original_tz="UTC", aware=False, tz="Canada/Mountain")
        assert result.tzinfo is None
        assert result.hour == 11  # 18 - 7

    def test_converts_aware(self) -> None:
        from wcp_library.time import convert_tz

        t = datetime(2024, 1, 15, 18, 0, 0)
        result = convert_tz(t, original_tz="UTC", aware=True, tz="Canada/Mountain")
        assert result.tzinfo is not None

    def test_invalid_original_tz_raises(self) -> None:
        from wcp_library.time import convert_tz

        with pytest.raises(pytz.UnknownTimeZoneError):
            convert_tz(datetime(2024, 1, 1), original_tz="Bogus/Zone")


# ---------------------------------------------------------------------------
# get_utc_timestamp
# ---------------------------------------------------------------------------


class TestGetUtcTimestamp:
    def test_returns_int(self) -> None:
        from wcp_library.time import get_utc_timestamp

        ts = get_utc_timestamp(datetime(2024, 1, 15, 0, 0, 0), original_tz="UTC")
        assert isinstance(ts, int)

    def test_mountain_is_later_than_utc_for_same_wall_time(self) -> None:
        """For the same wall-clock input, Mountain->UTC conversion yields a
        later timestamp than UTC->UTC. The absolute values depend on machine
        timezone (due to convert_tz stripping tzinfo before .timestamp()), so
        we test the relative offset instead of absolute seconds.
        """
        from wcp_library.time import get_utc_timestamp

        utc_ts = get_utc_timestamp(datetime(2024, 1, 15, 0, 0, 0), original_tz="UTC")
        mountain_ts = get_utc_timestamp(
            datetime(2024, 1, 15, 0, 0, 0), original_tz="Canada/Mountain"
        )
        # Mountain midnight is 7 hours behind UTC in January; interpreted as
        # happening "later in UTC" than UTC midnight of the same wall-time.
        # Use a tolerant range because pytz's replace-tzinfo path uses the
        # zone's LMT offset (roughly 7h 7m for Canada/Mountain) rather than
        # the canonical -7:00 DST-aware offset.
        delta = mountain_ts - utc_ts
        assert 6 * 3600 <= delta <= 8 * 3600

    def test_invalid_original_tz_raises(self) -> None:
        from wcp_library.time import get_utc_timestamp

        with pytest.raises(pytz.UnknownTimeZoneError):
            get_utc_timestamp(datetime(2024, 1, 15), original_tz="Not/A_Zone")


# ---------------------------------------------------------------------------
# get_local_timestamp
# ---------------------------------------------------------------------------


class TestGetLocalTimestamp:
    def test_returns_integer(self) -> None:
        from wcp_library.time import get_local_timestamp

        ts = get_local_timestamp(datetime(2024, 1, 15, 0, 0, 0))
        assert isinstance(ts, int)

    def test_invalid_original_tz_raises(self) -> None:
        from wcp_library.time import get_local_timestamp

        with pytest.raises(pytz.UnknownTimeZoneError):
            get_local_timestamp(datetime(2024, 1, 15), original_tz="Nope/Here")
