"""Fetch an ordered CSV snapshot from SODA for local use or the Bronze layer."""
import argparse
import csv
import hashlib
import io
import json
import logging
import os
import ssl
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from pipeline.config import settings

V2_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
V3_URL = "https://data.cityofchicago.org/api/v3/views/wrvz-psew/query.csv"
log = logging.getLogger(__name__)


def ensure_bucket():
    # A local download needs only the Python standard library, not boto3 or MinIO.
    from pipeline.storage import ensure_bucket as create_bucket
    return create_bucket()


def fetch_page(offset, config):
    headers = {"User-Agent": "artefact-taxi-pipeline/1.0"}
    if config.token:
        headers["X-App-Token"] = config.token
    if config.api_version == "3":
        if not config.token:
            raise ValueError("SODA v3 requires an application token: set SODA_APP_TOKEN. "
                             "Alternatively set SODA_API_VERSION=2 for the public v2 endpoint.")
        query = (f"SELECT * WHERE trip_start_timestamp >= '{config.start}T00:00:00' "
                 f"AND trip_start_timestamp < '{config.end}T00:00:00' "
                 "ORDER BY trip_start_timestamp, trip_id")
        body = json.dumps({"query": query,
                           "page": {"pageNumber": offset // config.page_size + 1,
                                    "pageSize": config.page_size},
                           "includeSystem": False, "includeSynthetic": False}).encode("utf-8")
        headers["Content-Type"] = "application/json"
        request = Request(V3_URL, data=body, headers=headers, method="POST")
    else:
        params = {"$where": f"trip_start_timestamp >= '{config.start}T00:00:00' AND trip_start_timestamp < '{config.end}T00:00:00'",
                  "$order": "trip_start_timestamp, trip_id", "$limit": config.page_size, "$offset": offset}
        request = Request(f"{V2_URL}?{urlencode(params)}", headers=headers)
    for attempt in range(5):
        try:
            with urlopen(request, timeout=120) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, URLError) and isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise RuntimeError(
                    "TLS certificate verification failed. Configure a trusted CA bundle "
                    "for Python; on macOS, try SSL_CERT_FILE=/etc/ssl/cert.pem."
                ) from exc
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 4:
                raise
            log.warning("SODA request failed (%s: %s); retry %s/4", type(exc).__name__, exc, attempt + 1)
            time.sleep(2 ** attempt)


def download_pages(folder, config):
    """Save unchanged CSV pages; return their paths and extraction metadata."""
    offset = 0
    pages = []
    total = 0
    expected_header = None
    metadata_pages = []
    while True:
        data = fetch_page(offset, config)
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, None)
        if not header:
            raise ValueError("SODA returned a CSV without a header")
        if expected_header is not None and header != expected_header:
            raise ValueError("SODA CSV headers changed between pages")
        expected_header = header
        count = sum(1 for _ in reader)
        if not count:
            break
        filename = Path(folder) / f"part-{len(pages):05d}.csv"
        filename.write_bytes(data)
        pages.append(filename)
        metadata_pages.append({"file": filename.name, "rows": count,
                               "sha256": hashlib.sha256(data).hexdigest()})
        total += count
        log.info("Downloaded %s rows in %s pages", total, len(pages))
        if count < config.page_size:
            break
        offset += count
    if not total:
        raise ValueError("No trips returned for the requested period")
    metadata = {"source": V3_URL if config.api_version == "3" else V2_URL,
                "api_version": config.api_version, "start_date": config.start, "end_date_exclusive": config.end,
                "page_size": config.page_size, "order": "trip_start_timestamp, trip_id",
                "rows": total, "columns": expected_header, "pages": metadata_pages}
    return pages, metadata


def download_to_local(output_dir):
    """Download a complete snapshot to a new local directory, without overwriting files."""
    config = settings()
    target = Path(output_dir).resolve()
    if target.exists():
        raise FileExistsError(f"Output directory already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{target.name}-", dir=target.parent) as staging:
        snapshot = Path(staging) / "snapshot"
        snapshot.mkdir()
        _, metadata = download_pages(snapshot, config)
        (snapshot / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        # No partial snapshot is presented as complete if extraction fails.
        if target.exists():
            raise FileExistsError(f"Output directory already exists: {target}")
        os.rename(snapshot, target)
    return target, metadata


def download_to_bronze(run_id=None):
    config = settings()
    run_id = run_id or uuid4().hex
    prefix = f"bronze/trips/start={config.start}/end={config.end}/run_id={run_id}"
    s3 = ensure_bucket()
    with tempfile.TemporaryDirectory() as folder:
        pages, metadata = download_pages(folder, config)
        for path in pages:
            s3.upload_file(str(path), config.bucket, f"{prefix}/{path.name}")
    return f"s3a://{config.bucket}/{prefix}/", metadata["rows"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a filtered and paginated Chicago Taxi CSV snapshot")
    parser.add_argument("--output-dir", help="Save CSV pages and a manifest locally in a new directory")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.output_dir:
        path, metadata = download_to_local(args.output_dir)
        print(f"Downloaded {metadata['rows']} rows in {len(metadata['pages'])} CSV files to {path}")
    else:
        print(download_to_bronze())
