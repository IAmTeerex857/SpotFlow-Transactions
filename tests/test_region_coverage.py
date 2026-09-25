import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Spotflow transactions data"))

from analyze_transactions import REGION_SET as REPORT_REGIONS  # noqa: E402
from anonymize_customers import REGIONS as ANONYMIZER_REGIONS  # noqa: E402
from process_june_v2 import REGIONS as CLEANER_REGIONS, _normalise_region  # noqa: E402


class RegionCoverageTest(unittest.TestCase):
    def test_new_regions_are_supported_throughout_pipeline(self):
        required = {"Sierra Leone", "Democratic Republic of the Congo", "Zambia"}

        self.assertTrue(required <= CLEANER_REGIONS)
        self.assertTrue(required <= ANONYMIZER_REGIONS)
        self.assertTrue(required <= REPORT_REGIONS)

    def test_corrupted_cote_divoire_is_normalized(self):
        self.assertEqual(_normalise_region("C\ufffd\ufffdte d'Ivoire"), "Côte d'Ivoire")


if __name__ == "__main__":
    unittest.main()
