import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Settings:
    start: str
    end: str
    page_size: int
    endpoint: str
    bucket: str
    token: str
    api_version: str


def settings() -> Settings:
    start = os.getenv("START_DATE", "2023-01-01")
    end = os.getenv("END_DATE", "2023-01-08")
    # Validate before these values are used in a SoQL expression or S3 key.
    if date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end:
        raise ValueError("Dates must use YYYY-MM-DD")
    if start >= end:
        raise ValueError("START_DATE must precede END_DATE (exclusive)")
    size = int(os.getenv("PAGE_SIZE", "10000"))
    if not 1 <= size <= 50000:
        raise ValueError("PAGE_SIZE must be between 1 and 50000")
    version = os.getenv("SODA_API_VERSION", "2")
    if version not in ("2", "3"):
        raise ValueError("SODA_API_VERSION must be 2 or 3")
    return Settings(start, end, size, os.getenv("S3_ENDPOINT", "http://minio:9000"),
                    os.getenv("S3_BUCKET", "taxi-lake"), os.getenv("SODA_APP_TOKEN", ""), version)
