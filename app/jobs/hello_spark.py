from app.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("hello-spark")

    data = [
        {"id": 1, "name": "brewery_a", "city": "Curitiba"},
        {"id": 2, "name": "brewery_b", "city": "Vitoria"},
    ]

    df = spark.createDataFrame(data)

    print("Schema:")
    df.printSchema()

    print("Data:")
    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()