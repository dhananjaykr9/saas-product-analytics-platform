import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    to_timestamp,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

RAW_S3_PATH = "s3://de-saas-platform-san/raw/"
PROCESSED_S3_PATH = "s3://de-saas-platform-san/processed/"

raw_df = spark.read.json(RAW_S3_PATH)

events_df = raw_df.select(explode(col("events")).alias("event")).select("event.*")


validated_df = events_df.dropna(
    subset=[
        "event_id",
        "user_id",
        "organization_id",
        "event_type",
        "country",
        "subscription_plan",
        "timestamp",
    ]
)

deduplicated_df = validated_df.dropDuplicates(["event_id"])

transformed_df = (
    deduplicated_df
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("ingestion_timestamp", current_timestamp())
)

output_df = (
    transformed_df
    .withColumn("year", col("timestamp").cast("date").cast("string").substr(1, 4))
    .withColumn("month", col("timestamp").cast("date").cast("string").substr(6, 2))
    .withColumn("day", col("timestamp").cast("date").cast("string").substr(9, 2))
)

(
    output_df.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(PROCESSED_S3_PATH)
)

job.commit()


