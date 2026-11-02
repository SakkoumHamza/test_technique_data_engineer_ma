import hashlib
import importlib
import json
import ssl
import tempfile
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from urllib.error import URLError

storage = types.ModuleType("pipeline.storage")
storage.ensure_bucket = lambda: None
with patch.dict(sys.modules, {"pipeline.storage": storage}):
    download = importlib.import_module("pipeline.download")


class FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, filename, bucket, key):
        with open(filename, "rb") as f:
            self.uploads.append((bucket, key, f.read()))


class DownloadTest(unittest.TestCase):
    def test_paginated_pages_preserve_csv_and_quoted_newlines(self):
        s3 = FakeS3()
        page1 = b'id,note\n1,"two\nlines"\n2,abc\n'
        page2 = b'id,note\n3,end\n'
        offsets = []

        def fetch(offset, config):
            offsets.append(offset)
            return [page1, page2][len(offsets) - 1]

        with patch.dict("os.environ", {"PAGE_SIZE": "2"}), \
             patch.object(download, "fetch_page", side_effect=fetch), \
             patch.object(download, "ensure_bucket", return_value=s3):
            path, count = download.download_to_bronze("test-run")
        self.assertEqual(offsets, [0, 2])
        self.assertEqual(count, 3)
        self.assertEqual([x[2] for x in s3.uploads], [page1, page2])
        self.assertTrue(path.endswith("run_id=test-run/"))

    def test_failed_download_does_not_upload_partial_snapshot(self):
        s3 = FakeS3()
        with patch.dict("os.environ", {"PAGE_SIZE": "1"}), \
             patch.object(download, "fetch_page", side_effect=[b'id\n1\n', RuntimeError("network")]), \
             patch.object(download, "ensure_bucket", return_value=s3):
            with self.assertRaisesRegex(RuntimeError, "network"):
                download.download_to_bronze("failed-run")
        self.assertEqual(s3.uploads, [])

    def test_local_download_preserves_pages_and_records_checksums(self):
        pages = [b'id,note\n1,"two\nlines"\n2,abc\n', b'id,note\n3,end\n']
        with tempfile.TemporaryDirectory() as folder, \
             patch.dict("os.environ", {"PAGE_SIZE": "2"}), \
             patch.object(download, "fetch_page", side_effect=pages):
            target, metadata = download.download_to_local(Path(folder) / "snapshot")
            manifest = json.loads((target / "manifest.json").read_text())
            self.assertEqual(metadata, manifest)
            self.assertEqual(manifest["rows"], 3)
            self.assertEqual(manifest["pages"][0]["sha256"], hashlib.sha256(pages[0]).hexdigest())
            self.assertEqual((target / "part-00000.csv").read_bytes(), pages[0])
            self.assertEqual((target / "part-00001.csv").read_bytes(), pages[1])
            with self.assertRaises(FileExistsError):
                download.download_to_local(target)

    def test_local_failure_does_not_leave_a_partial_snapshot(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.dict("os.environ", {"PAGE_SIZE": "1"}), \
             patch.object(download, "fetch_page", side_effect=[b'id\n1\n', RuntimeError("network")]):
            target = Path(folder) / "snapshot"
            with self.assertRaisesRegex(RuntimeError, "network"):
                download.download_to_local(target)
            self.assertFalse(target.exists())
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_changed_headers_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.dict("os.environ", {"PAGE_SIZE": "1"}), \
             patch.object(download, "fetch_page", side_effect=[b'id\n1\n', b'other\n2\n']):
            with self.assertRaisesRegex(ValueError, "headers changed"):
                download.download_to_local(Path(folder) / "snapshot")

    def test_v3_posts_filtered_paginated_csv_query(self):
        from pipeline.config import settings
        from unittest.mock import MagicMock
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"trip_id\n1\n"
        with patch.dict("os.environ", {"SODA_API_VERSION": "3", "SODA_APP_TOKEN": "test-token",
                                       "START_DATE": "2023-01-01", "END_DATE": "2023-02-01", "PAGE_SIZE": "2"}, clear=True), \
             patch.object(download, "urlopen", return_value=response) as open_url:
            self.assertEqual(download.fetch_page(2, settings()), b"trip_id\n1\n")
        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, download.V3_URL)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("X-app-token"), "test-token")
        body = json.loads(request.data)
        self.assertEqual(body["page"], {"pageNumber": 2, "pageSize": 2})
        self.assertIn("trip_start_timestamp >= '2023-01-01T00:00:00'", body["query"])
        self.assertIn("trip_start_timestamp < '2023-02-01T00:00:00'", body["query"])
        self.assertIn("ORDER BY trip_start_timestamp, trip_id", body["query"])

    def test_v3_missing_token_fails_with_actionable_message(self):
        from pipeline.config import settings
        with patch.dict("os.environ", {"SODA_API_VERSION": "3"}, clear=True):
            with self.assertRaisesRegex(ValueError, "SODA_APP_TOKEN"):
                download.fetch_page(0, settings())

    def test_v2_fallback_does_not_require_token(self):
        from pipeline.config import settings
        from unittest.mock import MagicMock
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"trip_id\n1\n"
        with patch.dict("os.environ", {"SODA_API_VERSION": "2"}, clear=True), \
             patch.object(download, "urlopen", return_value=response) as open_url:
            download.fetch_page(0, settings())
        request = open_url.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertTrue(request.full_url.startswith(download.V2_URL))

    def test_certificate_error_fails_without_retries(self):
        from pipeline.config import settings
        cause = ssl.SSLCertVerificationError("certificate verify failed")
        with patch.dict("os.environ", {"SODA_API_VERSION": "2"}, clear=True), \
             patch.object(download, "urlopen", side_effect=URLError(cause)) as open_url, \
             patch.object(download.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "TLS certificate verification failed"):
                download.fetch_page(0, settings())
            self.assertEqual(open_url.call_count, 1)
            sleep.assert_not_called()

    def test_invalid_dates_and_page_size_are_rejected(self):
        from pipeline.config import settings
        for env in ({"START_DATE": "2023-02-01", "END_DATE": "2023-01-01"},
                    {"START_DATE": "2023-01-01' OR 1=1", "END_DATE": "2023-02-01"},
                    {"PAGE_SIZE": "0"}, {"SODA_API_VERSION": "4"}):
            with self.subTest(env=env), patch.dict("os.environ", env, clear=True):
                with self.assertRaises(ValueError):
                    settings()


if __name__ == "__main__":
    unittest.main()
