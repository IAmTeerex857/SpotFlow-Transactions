import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Spotflow transactions data"))

from analyze_transactions import (  # noqa: E402
    REGION_SET as REPORT_REGIONS,
    STATUSES as REPORT_STATUSES,
    extract_transactions,
)
from anonymize_customers import REGIONS as ANONYMIZER_REGIONS, STATUSES as ANONYMIZER_STATUSES  # noqa: E402
from process_june_v2 import REGIONS as CLEANER_REGIONS, STATUSES as CLEANER_STATUSES, _normalise_region  # noqa: E402


class RegionCoverageTest(unittest.TestCase):
    def test_new_regions_are_supported_throughout_pipeline(self):
        required = {"Sierra Leone", "Democratic Republic of the Congo", "Zambia"}

        self.assertTrue(required <= CLEANER_REGIONS)
        self.assertTrue(required <= ANONYMIZER_REGIONS)
        self.assertTrue(required <= REPORT_REGIONS)

    def test_corrupted_cote_divoire_is_normalized(self):
        self.assertEqual(_normalise_region("C\ufffd\ufffdte d'Ivoire"), "Côte d'Ivoire")

    def test_refunded_status_is_supported_throughout_pipeline(self):
        for statuses in (CLEANER_STATUSES, ANONYMIZER_STATUSES, REPORT_STATUSES):
            self.assertIn("refunded", statuses)

    def test_status_like_message_does_not_hide_payment_date(self):
        row = [
            "paystack", "Nigeria", "customer 1", "successful", "", "card",
            "LIVE", "successful", "2026-10-01T12:34:56Z",
        ]

        transaction = list(extract_transactions(row, "<missing-date>"))[0]

        self.assertEqual(transaction[4], "successful")
        self.assertEqual(transaction[5], "2026-10-01T12:34:56Z")


if __name__ == "__main__":
    unittest.main()
