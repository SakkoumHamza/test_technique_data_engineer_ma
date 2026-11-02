import importlib
import sys
import types
import unittest
from unittest.mock import patch

from pyspark.sql import SparkSession

storage = types.ModuleType("pipeline.storage")
storage.client = lambda: None
with patch.dict(sys.modules, {"pipeline.storage": storage}):
    processing = importlib.import_module("pipeline.processing")


class ProcessingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("taxi-tests")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.ansi.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_cleaning_and_deterministic_deduplication(self):
        columns = ["Trip ID", "Trip Start Timestamp", "Trip Seconds", "Trip Miles",
                   "Pickup Community Area", "Fare", "Tips", "Trip Total"]
        good = ("a", "2023-01-01T10:00:00.000", "120", "1.2", "8", "5.00", "1.00", "6.00")
        different = ("a", "2023-01-01T10:00:00.000", "180", "1.2", "8", "5.00", "1.00", "6.00")
        bad_duration = ("b", "2023-01-01T10:00:00.000", "-10", "1.2", "8", "5.00", "1.00", "6.00")
        bad_total = ("c", "2023-01-01T10:00:00.000", "60", "1.2", "8", "5.00", "1.00", "-1")
        rows = [good, different, good, bad_duration, bad_total]
        first = processing.clean_trips(self.spark.createDataFrame(rows, columns)).collect()
        second = processing.clean_trips(self.spark.createDataFrame(list(reversed(rows)), columns)).collect()
        self.assertEqual(len(first), 1)
        self.assertEqual(first, second)
        self.assertEqual(first[0].trip_id, "a")
        self.assertEqual(str(first[0].trip_date), "2023-01-01")

    def test_gold_aggregates_include_null_zone_in_daily_only(self):
        columns = ["Trip ID", "Trip Start Timestamp", "Trip Seconds", "Trip Miles",
                   "Pickup Community Area", "Fare", "Tips", "Trip Total"]
        frame = self.spark.createDataFrame([
            ("a", "2023-01-01T10:00:00", "120", "1", "8", "5", "1", "6"),
            ("b", "2023-01-01T11:00:00", "60", "2", None, "4", "0", "4"),
            ("c", "2023-01-02T11:00:00", "30", "2", "8", "4", "0", "4"),
        ], columns)
        daily, zones = processing.gold_tables(processing.clean_trips(frame))
        days = {str(r.trip_date): r for r in daily.collect()}
        self.assertEqual(days["2023-01-01"].trip_count, 2)
        self.assertEqual(str(days["2023-01-01"].revenue_usd), "10.00")
        self.assertEqual(days["2023-01-01"].avg_duration_seconds, 90.0)
        zone = zones.collect()
        self.assertEqual(len(zone), 1)
        self.assertEqual(zone[0].trip_count, 2)
        self.assertEqual(str(zone[0].revenue_usd), "10.00")

    def test_missing_columns_fail_loudly(self):
        frame = self.spark.createDataFrame([("a",)], ["Trip ID"])
        with self.assertRaisesRegex(ValueError, "Missing CSV columns"):
            processing.clean_trips(frame)


if __name__ == "__main__":
    unittest.main()
