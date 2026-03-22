from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from app.clients.open_brewery import BreweryAPIClient
from app.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR
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


def build_run_id(reference: datetime | None = None) -> str:
    now = reference or datetime.now(UTC)
    return now.strftime("%Y%m%dT%H%M%SZ")


def ensure_data_dirs() -> None:
    for path in (BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        path.mkdir(parents=True, exist_ok=True)


def bronze_output_path(run_id: str) -> Path:
    ingestion_date = run_id[:8]
    return BRONZE_DIR / f"ingestion_date={ingestion_date}" / f"run_id={run_id}" / "breweries.json"


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

    client = BreweryAPIClient()
    records = client.fetch_all()
    persist_bronze(records, bronze_path)

    spark = get_spark("bees-breweries-pipeline")
    bronze_df = spark.read.schema(RAW_SCHEMA).json(str(bronze_path))
    silver_df = transform_silver(bronze_df)
    gold_df = aggregate_gold(silver_df)

    write_silver(silver_df, silver_path)
    write_gold(gold_df, gold_path)
    spark.stop()

    return PipelineArtifacts(
        run_id=pipeline_run_id,
        bronze_path=bronze_path,
        silver_path=silver_path,
        gold_path=gold_path,
    )
