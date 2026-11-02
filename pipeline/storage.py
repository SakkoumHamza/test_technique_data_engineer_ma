import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from pipeline.config import settings


def client():
    return boto3.client("s3", endpoint_url=settings().endpoint, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))


def ensure_bucket():
    s3 = client()
    bucket = settings().bucket
    for attempt in range(30):
        try:
            try:
                s3.head_bucket(Bucket=bucket)
            except ClientError as exc:
                if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                    raise
                s3.create_bucket(Bucket=bucket)
            return s3
        except EndpointConnectionError:
            if attempt == 29:
                raise
            time.sleep(2)
