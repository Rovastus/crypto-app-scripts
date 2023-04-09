import unittest as ut
import pandas as pd
import constant as const
import refactor.kraken_export_refactor as ker


class KrakenExportRefactorTests(ut.TestCase):
    def test_refactor(self):
        test_df = pd.read_csv("tests/data/kraken/refactor/refactor.csv")
        export_refactor = ker.KrakenExportRefactor()
        export_refactor.refactor(test_df)

        self.assertEqual(len(test_df), 11, "Unexpected size")

        expected_results = [
            {"asset": "BTC", "fee": 0.0},
            {"asset": "DOT", "fee": 0.0},
            {"asset": "SOL", "fee": 0.0},
            {"asset": "KSM", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
            {"asset": "ETH", "fee": 0.0},
            {"asset": "EUR", "fee": 1.0399},
            {"asset": "BTC", "fee": 0.0},
            {"asset": "ETH", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
        ]

        for i, element in enumerate(expected_results):
            self.assertEqual(
                test_df[const.KRAKEN_ASSET_COLUMN][i],
                element["asset"],
                f"Unexpected asset on index {i}",
            )
            self.assertEqual(
                test_df[const.KRAKEN_FEE_COLUMN][i],
                element["fee"],
                f"Unexpected asset on index {i}",
            )
