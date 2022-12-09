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
            "BTC",
            "DOT",
            "SOL",
            "KSM",
            "ETH2",
            "ETH",
            "EUR",
            "BTC",
            "ETH",
            "ETH2",
            "ETH2",
        ]

        for i, element in enumerate(expectedResults):
            self.assertEqual(
                test_df[const.KRAKEN_ASSET_COLUMN][i],
                element,
                "Unexpected asset on index {}".format(i),
            )
