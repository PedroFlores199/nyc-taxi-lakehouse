from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "NYCTaxiLakehouse") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .getOrCreate()
    )
