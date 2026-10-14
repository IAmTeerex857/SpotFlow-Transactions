import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Spotflow transactions data"))

from analyze_transactions import (  # noqa: E402
    REGION_SET as REPORT_REGIONS,
    STATUSES as REPORT_STATUSES,
    _normalise_message,
    extract_transactions,
)
from anonymize_customers import REGIONS as ANONYMIZER_REGIONS, STATUSES as ANONYMIZER_STATUSES  # noqa: E402
from process_june_v2 import REGIONS as CLEANER_REGIONS, STATUSES as CLEANER_STATUSES, _normalise_region  # noqa: E402
from render_html_report_v2 import Transaction, parse_csvs_v2  # noqa: E402


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

    def test_provider_message_secrets_are_redacted(self):
        token = "-".join(("12345678", "1234", "1234", "1234", "123456789abc"))

        self.assertEqual(
            _normalise_message(f"Token {token} is locked"),
            "Token [redacted] is locked",
        )
        self.assertNotIn(token, _normalise_message(f"Authorization: Bearer {token}"))

    def test_equivalent_provider_errors_are_grouped(self):
        insufficient_funds = {
            _normalise_message("Insufficient funds"),
            _normalise_message("Payment failed. The customer doesn't have enough funds"),
            _normalise_message("The customer does not have enough funds to complete the payment"),
        }
        no_reason = {
            _normalise_message("Payment failed. No reason was provided by the provider"),
            _normalise_message("The provider 'orange SLE' did not specify a reason for this failure"),
        }

        self.assertEqual(insufficient_funds, {"Insufficient funds"})
        self.assertEqual(no_reason, {"Provider returned no failure reason"})

    def test_distinct_provider_errors_remain_separate(self):
        self.assertNotEqual(
            _normalise_message("Insufficient funds"),
            _normalise_message("The customer does not have enough funds to complete the payment or has reached a payment limit"),
        )

    def test_usd_volume_does_not_require_conversion_rate(self):
        transaction = Transaction(
            merchant="Merchant",
            amount=25,
            currency="USD",
            rate=0,
            provider="paystack",
            region="Nigeria",
            status="successful",
            channel="card",
            message="Success",
            dt=None,
            customer="customer 1",
        )

        self.assertEqual(transaction.amount_usd, 25)

    def test_daily_v2_results_merge_without_double_counting(self):
        header = "Merchant,Amount,Currency,Rate,Provider,Region,Customer,Payment Status,Payment Channel,Provider Message,Payment Date\n"
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.csv"
            second = Path(directory) / "second.csv"
            first.write_text(header + "One,10,USD,,paystack,Nigeria,customer 1,successful,card,Success,2026-10-01T12:00:00Z\n", encoding="utf-8")
            second.write_text(header + "Two,20,USD,,paystack,Ghana,customer 1,failed,card,Insufficient funds,2026-10-02T12:00:00Z\n", encoding="utf-8")

            transactions, summaries, merchants = parse_csvs_v2([first, second])

        self.assertEqual(len(transactions), 2)
        self.assertEqual(sum(summaries["Aggregate"].status_counts.values()), 2)
        self.assertEqual(sum(merchant.total_txns for merchant in merchants.values()), 2)
        self.assertEqual(summaries["Aggregate"].successful_volume_usd, 10)


if __name__ == "__main__":
    unittest.main()
