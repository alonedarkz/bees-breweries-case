from datetime import UTC, datetime

from app.pipeline import bronze_output_path, build_run_id, sanitize_partition_value


def test_build_run_id_uses_utc_format():
    reference = datetime(2026, 3, 21, 12, 30, 45, tzinfo=UTC)
    assert build_run_id(reference) == "20260321T123045Z"


def test_bronze_output_path_uses_date_partition():
    path = bronze_output_path("20260321T123045Z")
    assert "ingestion_date=20260321" in str(path)
    assert "run_id=20260321T123045Z" in str(path)


def test_sanitize_partition_value_defaults_to_unknown():
    assert sanitize_partition_value(None) == "unknown"
    assert sanitize_partition_value("  Sao Paulo ") == "sao_paulo"
