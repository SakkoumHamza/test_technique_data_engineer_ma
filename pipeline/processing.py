import json
import os
import re

from pyspark.sql import SparkSession, Window, functions as F

from pipeline.config import settings
from pipeline.storage import client

# =============================================================
# Spark transformations and publication of a complete analytical snapshot.
# =============================================================


def spark_session():
    config = settings()
    return (SparkSession.builder.appName("chicago-taxi")
            .master("local[2]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.hadoop.fs.s3a.endpoint", config.endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .getOrCreate())


def normalize_columns(frame):
    names = [re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") for name in frame.columns]
    if len(names) != len(set(names)):
        raise ValueError("CSV headers are ambiguous after normalization")
    return frame.toDF(*names)


def clean_trips(raw):
    raw = normalize_columns(raw)
    required = {"trip_id", "trip_start_timestamp", "trip_seconds", "trip_miles", "pickup_community_area", "fare", "tips", "trip_total"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Missing CSV columns: {sorted(missing)}")
    typed = raw.select(
        F.trim("trip_id").alias("trip_id"),
        F.to_timestamp("trip_start_timestamp").alias("trip_start_timestamp"),
        F.col("trip_seconds").cast("long").alias("trip_seconds"),
        F.col("trip_miles").cast("double").alias("trip_miles"),
        F.col("pickup_community_area").cast("int").alias("pickup_community_area"),
        F.col("fare").cast("decimal(12,2)").alias("fare"),
        F.col("tips").cast("decimal(12,2)").alias("tips"),
        F.col("trip_total").cast("decimal(12,2)").alias("trip_total"),
    )
    valid = typed.filter(
        F.col("trip_id").isNotNull() & (F.length("trip_id") > 0)
        & F.col("trip_start_timestamp").isNotNull()
        & F.col("trip_seconds").isNotNull() & (F.col("trip_seconds") > 0)
        & F.col("trip_total").isNotNull() & (F.col("trip_total") >= 0)
        & (F.col("trip_miles").isNull() | (F.col("trip_miles") >= 0))
        & (F.col("fare").isNull() | (F.col("fare") >= 0))
        & (F.col("tips").isNull() | (F.col("tips") >= 0))
    )
    # Stable tie-breaker for conflicting rows sharing an ID, independent of partition order.
    digest = F.sha2(F.to_json(F.struct(*[F.col(c) for c in valid.columns])), 256)
    ranked = valid.withColumn("_rank", F.row_number().over(Window.partitionBy("trip_id").orderBy(digest)))
    return ranked.filter(F.col("_rank") == 1).drop("_rank").withColumn("trip_date", F.to_date("trip_start_timestamp"))


def gold_tables(clean):
    daily = clean.groupBy("trip_date").agg(
        F.count("*").alias("trip_count"),
        F.sum("trip_total").alias("revenue_usd"),
        F.round(F.avg("trip_seconds"), 2).alias("avg_duration_seconds"))
    zones = (clean.filter(F.col("pickup_community_area").isNotNull())
             .groupBy("pickup_community_area").agg(
                 F.count("*").alias("trip_count"), F.sum("trip_total").alias("revenue_usd")))
    return daily, zones


def process(bronze_path, run_id, bronze_count, log):
    config = settings()
    root = f"s3a://{config.bucket}"
    silver = f"{root}/silver/trips/run_id={run_id}/"
    daily = f"{root}/gold/daily_metrics/run_id={run_id}/"
    zones = f"{root}/gold/pickup_zones/run_id={run_id}/"
    spark = spark_session()
    try:
        raw = (spark.read.option("header", True).option("multiLine", True)
               .option("mode", "FAILFAST").csv(bronze_path))
        raw_count = raw.count()
        if raw_count != bronze_count:
            raise ValueError(f"CSV count mismatch: downloaded {bronze_count}, Spark read {raw_count}")
        clean = clean_trips(raw).cache()
        clean_count = clean.count()
        if clean_count == 0:
            raise ValueError("No valid trips remain after quality checks")
        clean.write.mode("errorifexists").partitionBy("trip_date").parquet(silver)
        daily_table, zone_table = gold_tables(clean)
        daily_table.write.mode("errorifexists").parquet(daily)
        zone_table.write.mode("errorifexists").parquet(zones)
        manifest = {"run_id": run_id, "start_date": config.start, "end_date_exclusive": config.end,
                    "bronze_rows": raw_count, "silver_rows": clean_count,
                    "rejected_or_duplicate_rows": raw_count - clean_count,
                    "daily_prefix": f"gold/daily_metrics/run_id={run_id}/",
                    "zones_prefix": f"gold/pickup_zones/run_id={run_id}/"}
        # One S3 object switches both tables together only after both writes succeed.
        client().put_object(Bucket=config.bucket, Key="gold/latest.json",
                            Body=json.dumps(manifest).encode(), ContentType="application/json")
        log.info("Published snapshot: %s", manifest)
        return manifest
    finally:
        spark.stop()
