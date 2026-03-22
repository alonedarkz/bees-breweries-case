from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from app.clients.open_brewery import BreweryAPIClient
from app.config import BRONZE_DIR, GOLD_DIR, MONITORING_DIR, SILVER_DIR
from app.exceptions import PipelineQualityError
from app.monitoring import emit_log, persist_run_report, utc_now_iso
from app.utils.spark_session import get_spark


RAW_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
        StructField("is_closed", BooleanType(), True),
    ]
)


@dataclass
class PipelineArtifacts:
    run_id: str
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    monitoring_path: Path


def build_run_id(reference: datetime | None = None) -> str:
    now = reference or datetime.now(UTC)
    return now.strftime("%Y%m%dT%H%M%SZ")


def ensure_data_dirs() -> None:
    for path in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, MONITORING_DIR):
        path.mkdir(parents=True, exist_ok=True)


def bronze_output_path(run_id: str) -> Path:
    ingestion_date = run_id[:8]
    return BRONZE_DIR / f"ingestion_date={ingestion_date}" / f"run_id={run_id}" / "breweries.json"


def monitoring_output_path(run_id: str) -> Path:
    return MONITORING_DIR / f"run_id={run_id}.json"


def sanitize_partition_value(value: str | None) -> str:
    if value is None:
        return "unknown"

    normalized = value.strip().lower().replace(" ", "_")
    return normalized or "unknown"


def persist_bronze(records: Iterable[dict[str, Any]], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8") as file_obj:
        for record in records:
            file_obj.write(json.dumps(record, ensure_ascii=True))
            file_obj.write("\n")


def load_bronze_dataframe(bronze_path: Path) -> DataFrame:
    spark = get_spark("bees-bronze-reader")
    return spark.read.schema(RAW_SCHEMA).json(str(bronze_path))


def validate_api_payload(records: list[dict[str, Any]]) -> None:
    if not records:
        raise PipelineQualityError("Open Brewery API returned zero records.")

    missing_ids = sum(1 for record in records if not str(record.get("id", "")).strip())
    if missing_ids > 0:
        raise PipelineQualityError(f"Open Brewery API returned {missing_ids} records without id.")


def collect_silver_metrics(df: DataFrame) -> dict[str, int]:
    metrics = (
        df.agg(
            F.count("*").alias("row_count"),
            F.sum(F.when(F.col("id").isNull() | (F.trim(F.col("id")) == ""), 1).otherwise(0)).alias(
                "null_id_count"
            ),
            F.sum(
                F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""), 1).otherwise(0)
            ).alias("null_name_count"),
            F.sum(
                F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), 1).otherwise(0)
            ).alias("null_country_count"),
            F.sum(
                F.when(
                    F.col("brewery_type").isNull() | (F.trim(F.col("brewery_type")) == ""),
                    1,
                ).otherwise(0)
            ).alias("null_brewery_type_count"),
            F.sum(
                F.when(
                    (F.col("city").isNull() | (F.trim(F.col("city")) == ""))
                    & (F.col("state").isNull() | (F.trim(F.col("state")) == ""))
                    & (F.col("country").isNull() | (F.trim(F.col("country")) == "")),
                    1,
                ).otherwise(0)
            ).alias("records_without_location_count"),
        )
        .first()
        .asDict()
    )
    return {key: int(value or 0) for key, value in metrics.items()}


def evaluate_silver_quality(source_record_count: int, metrics: dict[str, int]) -> dict[str, Any]:
    if source_record_count == 0 or metrics["row_count"] == 0:
        raise PipelineQualityError("Silver layer is empty after transformation.")

    if metrics["null_id_count"] > 0:
        raise PipelineQualityError("Silver layer contains null or blank ids.")

    if metrics["row_count"] > source_record_count:
        raise PipelineQualityError("Silver layer has more rows than the ingested raw payload.")

    warnings: list[str] = []
    if metrics["records_without_location_count"] > 0:
        warnings.append(
            f"{metrics['records_without_location_count']} records are missing city, state and country."
        )
    if metrics["null_country_count"] > 0:
        warnings.append(f"{metrics['null_country_count']} records are missing country.")
    if metrics["null_name_count"] > 0:
        warnings.append(f"{metrics['null_name_count']} records are missing brewery name.")
    if metrics["null_brewery_type_count"] > 0:
        warnings.append(f"{metrics['null_brewery_type_count']} records are missing brewery type.")

    return {
        **metrics,
        "duplicate_ids_removed": source_record_count - metrics["row_count"],
        "warnings": warnings,
    }


def collect_gold_metrics(df: DataFrame) -> dict[str, int]:
    metrics = (
        df.agg(
            F.count("*").alias("row_count"),
            F.sum("brewery_count").alias("aggregated_brewery_count"),
            F.countDistinct("brewery_type").alias("brewery_type_count"),
            F.countDistinct(
                F.concat_ws("||", F.col("country"), F.col("state"), F.col("city"))
            ).alias("location_count"),
        )
        .first()
        .asDict()
    )
    return {key: int(value or 0) for key, value in metrics.items()}


def evaluate_gold_quality(expected_total_count: int, metrics: dict[str, int]) -> dict[str, Any]:
    if metrics["row_count"] == 0:
        raise PipelineQualityError("Gold layer is empty after aggregation.")

    if metrics["aggregated_brewery_count"] != expected_total_count:
        raise PipelineQualityError(
            "Gold aggregated brewery count does not match the silver row count."
        )

    warnings: list[str] = []
    if metrics["location_count"] == 0:
        warnings.append("Gold layer does not contain any distinct location.")

    return {**metrics, "warnings": warnings}


def transform_silver(df: DataFrame) -> DataFrame:
    def normalized_partition(column_name: str):
        cleaned = F.lower(F.trim(F.col(column_name)))
        cleaned = F.regexp_replace(cleaned, r"\s+", "_")
        return F.when(cleaned.isNull() | (cleaned == ""), F.lit("unknown")).otherwise(cleaned)

    return (
        df.select(
            "id",
            "name",
            "brewery_type",
            "city",
            "state",
            "state_province",
            "country",
            "latitude",
            "longitude",
            "website_url",
            "is_closed",
        )
        .withColumn("brewery_type", F.lower(F.trim(F.col("brewery_type"))))
        .withColumn("city", F.lower(F.trim(F.col("city"))))
        .withColumn(
            "state",
            F.lower(F.trim(F.coalesce(F.col("state"), F.col("state_province")))),
        )
        .withColumn("country", F.lower(F.trim(F.col("country"))))
        .withColumn("city_partition", normalized_partition("city"))
        .withColumn("state_partition", normalized_partition("state"))
        .withColumn("country_partition", normalized_partition("country"))
        .dropDuplicates(["id"])
    )


def aggregate_gold(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("country", "state", "city", "brewery_type")
        .agg(F.count("*").alias("brewery_count"))
        .orderBy("country", "state", "city", "brewery_type")
    )


def write_silver(df: DataFrame, destination: Path) -> None:
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("country_partition", "state_partition")
        .parquet(str(destination))
    )


def write_gold(df: DataFrame, destination: Path) -> None:
    df.coalesce(1).write.mode("overwrite").parquet(str(destination))


def run_pipeline(run_id: str | None = None) -> PipelineArtifacts:
    ensure_data_dirs()

    pipeline_run_id = run_id or build_run_id()
    bronze_path = bronze_output_path(pipeline_run_id)
    silver_path = SILVER_DIR / f"run_id={pipeline_run_id}"
    gold_path = GOLD_DIR / f"run_id={pipeline_run_id}"
    run_report_path = monitoring_output_path(pipeline_run_id)
    pipeline_started_at = utc_now_iso()
    pipeline_start = perf_counter()
    stage_timings: dict[str, float] = {}
    spark = None

    emit_log("pipeline_started", run_id=pipeline_run_id)

    try:
        stage_start = perf_counter()
        client = BreweryAPIClient()
        records = client.fetch_all()
        stage_timings["api_fetch_seconds"] = round(perf_counter() - stage_start, 3)
        validate_api_payload(records)
        emit_log(
            "api_fetch_completed",
            run_id=pipeline_run_id,
            record_count=len(records),
            duration_seconds=stage_timings["api_fetch_seconds"],
        )

        stage_start = perf_counter()
        persist_bronze(records, bronze_path)
        stage_timings["bronze_write_seconds"] = round(perf_counter() - stage_start, 3)
        emit_log(
            "bronze_written",
            run_id=pipeline_run_id,
            bronze_path=str(bronze_path),
            duration_seconds=stage_timings["bronze_write_seconds"],
        )

        spark = get_spark("bees-breweries-pipeline")

        stage_start = perf_counter()
        bronze_df = spark.read.schema(RAW_SCHEMA).json(str(bronze_path))
        silver_df = transform_silver(bronze_df)
        silver_metrics = evaluate_silver_quality(len(records), collect_silver_metrics(silver_df))
        gold_df = aggregate_gold(silver_df)
        gold_metrics = evaluate_gold_quality(
            silver_metrics["row_count"],
            collect_gold_metrics(gold_df),
        )
        stage_timings["transform_and_quality_seconds"] = round(perf_counter() - stage_start, 3)
        emit_log(
            "quality_checks_passed",
            run_id=pipeline_run_id,
            duplicate_ids_removed=silver_metrics["duplicate_ids_removed"],
            records_without_location_count=silver_metrics["records_without_location_count"],
            aggregated_brewery_count=gold_metrics["aggregated_brewery_count"],
            duration_seconds=stage_timings["transform_and_quality_seconds"],
        )

        stage_start = perf_counter()
        write_silver(silver_df, silver_path)
        stage_timings["silver_write_seconds"] = round(perf_counter() - stage_start, 3)
        emit_log(
            "silver_written",
            run_id=pipeline_run_id,
            silver_path=str(silver_path),
            duration_seconds=stage_timings["silver_write_seconds"],
        )

        stage_start = perf_counter()
        write_gold(gold_df, gold_path)
        stage_timings["gold_write_seconds"] = round(perf_counter() - stage_start, 3)
        emit_log(
            "gold_written",
            run_id=pipeline_run_id,
            gold_path=str(gold_path),
            duration_seconds=stage_timings["gold_write_seconds"],
        )

        warnings = [*silver_metrics["warnings"], *gold_metrics["warnings"]]
        total_duration = round(perf_counter() - pipeline_start, 3)
        run_report = {
            "run_id": pipeline_run_id,
            "status": "SUCCESS",
            "started_at": pipeline_started_at,
            "finished_at": utc_now_iso(),
            "duration_seconds": total_duration,
            "stage_timings_seconds": stage_timings,
            "artifacts": {
                "bronze_path": str(bronze_path),
                "silver_path": str(silver_path),
                "gold_path": str(gold_path),
            },
            "raw_record_count": len(records),
            "quality": {
                "silver": {key: value for key, value in silver_metrics.items() if key != "warnings"},
                "gold": {key: value for key, value in gold_metrics.items() if key != "warnings"},
                "warnings": warnings,
            },
        }
        persist_run_report(run_report, run_report_path)
        emit_log(
            "pipeline_finished",
            run_id=pipeline_run_id,
            status="SUCCESS",
            duration_seconds=total_duration,
            monitoring_path=str(run_report_path),
            warnings_count=len(warnings),
        )

        return PipelineArtifacts(
            run_id=pipeline_run_id,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path,
            monitoring_path=run_report_path,
        )
    except Exception as exc:
        total_duration = round(perf_counter() - pipeline_start, 3)
        run_report = {
            "run_id": pipeline_run_id,
            "status": "FAILED",
            "started_at": pipeline_started_at,
            "finished_at": utc_now_iso(),
            "duration_seconds": total_duration,
            "stage_timings_seconds": stage_timings,
            "artifacts": {
                "bronze_path": str(bronze_path),
                "silver_path": str(silver_path),
                "gold_path": str(gold_path),
            },
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
        persist_run_report(run_report, run_report_path)
        emit_log(
            "pipeline_failed",
            run_id=pipeline_run_id,
            error_type=type(exc).__name__,
            error_message=str(exc),
            duration_seconds=total_duration,
            monitoring_path=str(run_report_path),
        )
        raise
    finally:
        if spark is not None:
            spark.stop()
