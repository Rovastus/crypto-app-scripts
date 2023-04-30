import unittest as ut
import pandas as pd
from solana_export import SolanaExport
import constant as const


class SolanaExportTest(ut.TestCase):
    def test_transaction(self):
        test_df = pd.read_csv("tests/data/solana/solana.csv")
        export = SolanaExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 2, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-12-09 17:47:15",
                const.EARN_OPERATION,
                const.SOLANA_EARN_DESCRIPTION,
                '{"amount": "0.01380075", "coin": "SOL"}',
            ],
            [
                1,
                "2022-12-06 22:31:38",
                const.TRANSFER_OPERATION,
                const.SOLANA_SPEND_DESCRIPTION,
                '{"fee": "5e-06", "coin": "SOL"}',
            ],
        ]:
            result_row = result_df.iloc[test_case[0]]
            self.assertEqual(
                result_row[const.UTC_TIME_COLUMN],
                test_case[1],
                "Unexpected utc_time",
            )
            self.assertEqual(
                result_row[const.OPERATION_COLUMN],
                test_case[2],
                "Unexpected operation",
            )
            self.assertEqual(
                result_row[const.DESCRIPTION_COLUMN],
                test_case[3],
                "Unexpected description",
            )
            self.assertEqual(
                result_row[const.DATA_COLUMN],
                test_case[4],
                "Unexpected data",
            )
