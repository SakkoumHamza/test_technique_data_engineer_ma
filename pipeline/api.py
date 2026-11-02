import io
import json

import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query

from pipeline.config import settings
from pipeline.storage import client

# =============================================================
# Read the latest complete Gold snapshot directly from object storage.
# =============================================================


app = FastAPI(title="Chicago Taxi BI API", version="1.0")


def manifest():
    try:
        return json.loads(client().get_object(Bucket=settings().bucket, Key="gold/latest.json")["Body"].read())
    except (ClientError, BotoCoreError) as exc:
        raise HTTPException(503, "No published snapshot or object storage unavailable") from exc


def rows(prefix):
    s3 = client()
    bucket = settings().bucket
    result = []
    try:
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    data = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                    result.extend(pq.read_table(io.BytesIO(data)).to_pylist())
    except (ClientError, BotoCoreError) as exc:
        raise HTTPException(503, "Cannot read the published snapshot") from exc
    return result


@app.get("/snapshot")
def snapshot():
    return manifest()


@app.get("/metrics/daily")
def daily_metrics():
    return sorted(rows(manifest()["daily_prefix"]), key=lambda row: row["trip_date"])


@app.get("/metrics/pickup-zones")
def pickup_zones(limit: int = Query(10, ge=1, le=77)):
    return sorted(rows(manifest()["zones_prefix"]), key=lambda row: (-row["trip_count"], row["pickup_community_area"]))[:limit]
