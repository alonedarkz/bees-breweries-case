from pyspark.sql import SparkSession


def get_spark(app_name: str = "bees-breweries-case") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark
