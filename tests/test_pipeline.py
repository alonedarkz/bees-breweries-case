from datetime import UTC, datetime

import pytest

from app.exceptions import PipelineQualityError
from app.pipeline import (
    bronze_output_path,
    build_run_id,
    evaluate_gold_quality,
    evaluate_silver_quality,
    monitoring_output_path,
    sanitize_partition_value,
    validate_api_payload,
)


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


def test_monitoring_output_path_uses_run_id():
    path = monitoring_output_path("20260321T123045Z")
    assert path.name == "run_id=20260321T123045Z.json"


def test_validate_api_payload_raises_when_empty():
    with pytest.raises(PipelineQualityError):
        validate_api_payload([])


def test_evaluate_silver_quality_returns_duplicate_and_warnings():
    report = evaluate_silver_quality(
        source_record_count=10,
        metrics={
            "row_count": 9,
            "null_id_count": 0,
            "null_name_count": 1,
            "null_country_count": 2,
            "null_brewery_type_count": 0,
            "records_without_location_count": 1,
        },
    )

    assert report["duplicate_ids_removed"] == 1
    assert len(report["warnings"]) == 3


def test_evaluate_gold_quality_raises_when_total_does_not_match():
    with pytest.raises(PipelineQualityError):
        evaluate_gold_quality(
            expected_total_count=10,
            metrics={
                "row_count": 3,
                "aggregated_brewery_count": 9,
                "brewery_type_count": 2,
                "location_count": 2,
            },
        )
