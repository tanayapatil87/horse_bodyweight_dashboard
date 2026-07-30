from datetime import datetime, timezone

from botocore.exceptions import NoCredentialsError

from dashboard_using_S3 import (
    S3Handler,
    _build_s3_prefix,
    _parse_s3_uri,
    analytics_available,
)


def test_parse_s3_uri() -> None:
    bucket, prefix = _parse_s3_uri("s3://example-bucket/path/to/data")

    assert bucket == "example-bucket"
    assert prefix == "path/to/data"


def test_build_s3_prefix() -> None:
    now = datetime(2026, 7, 28, 10, 30, tzinfo=timezone.utc)

    assert _build_s3_prefix("base", now) == (
        "base/horses/merged_fields/year=2026/month=07/day=28/"
    )


def test_analytics_available_returns_empty_when_credentials_are_missing(
    monkeypatch,
) -> None:
    def raise_no_credentials(*args, **kwargs):
        raise NoCredentialsError()

    monkeypatch.setattr("dashboard_using_S3.s3.list_objects_v2", raise_no_credentials)

    result = analytics_available()

    assert result.is_empty()
    assert result.columns == ["RaceKey", "AnalyticsAvailable"]


def test_s3_handler_reads_environment_settings(monkeypatch) -> None:
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_PATH", "s3://example-bucket/path")

    handler = S3Handler(profile_name="default")

    assert handler.storage_options["profile"] == "default"
    assert handler.storage_options["client_kwargs"]["region_name"] == "eu-west-1"
    assert handler._normalize_path("data/file.parquet") == (
        "s3://example-bucket/path/data/file.parquet"
    )
