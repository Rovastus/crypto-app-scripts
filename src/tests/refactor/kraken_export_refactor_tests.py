import unittest as ut
import pandas as pd
import src.constant as const
import src.refactor.kraken_export_refactor as ker


class KrakenExportRefactorTests(ut.TestCase):
    def test_refactor(self):
        test_df = pd.read_csv("src/tests/data/kraken/refactor.csv")
        exportRefactor = ker.KrakenExportRefactor()
        exportRefactor.refactor(test_df)

        self.assertEqual(len(test_df), 11, "Unexpected size")

        expectedResults = [
            {"asset": "BTC", "fee": 0.0},
            {"asset": "DOT", "fee": 0.0},
            {"asset": "SOL", "fee": 0.0},
            {"asset": "KSM", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
            {"asset": "ETH", "fee": 0.0},
            {"asset": "EUR", "fee": -1.0399},
            {"asset": "BTC", "fee": 0.0},
            {"asset": "ETH", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
            {"asset": "ETH2", "fee": 0.0},
        ]

        for i, element in enumerate(expectedResults):
            self.assertEqual(
                test_df[const.KRAKEN_ASSET_COLUMN][i],
                element["asset"],
                "Unexpected asset on index {}".format(i),
            )
            self.assertEqual(
                test_df[const.KRAKEN_FEE_COLUMN][i],
                element["fee"],
                "Unexpected fee on index {}".format(i),
            )
