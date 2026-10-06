from datetime import datetime, timedelta, timezone

from dashboard_refresh_policy import should_refresh


def test_should_refresh_initial_load():
    now = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
    assert should_refresh(None, now, 60) is True


def test_should_refresh_before_interval():
    now = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
    last_refresh = now - timedelta(seconds=30)
    assert should_refresh(last_refresh, now, 60) is False


def test_should_refresh_after_interval():
    now = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
    last_refresh = now - timedelta(seconds=61)
    assert should_refresh(last_refresh, now, 60) is True
