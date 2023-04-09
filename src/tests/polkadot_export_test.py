import unittest as ut
import pandas as pd
from polkadot_export import PolkadotExport
import constant as const


class PolkadotExportTest(ut.TestCase):
    def test_transaction(self):
        test_df = pd.read_csv("tests/data/polkadot/polkadot.csv")
        export = PolkadotExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 3, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-12-08 20:07:00",
                const.TRANSFER_OPERATION,
                const.POLKADOT_TRANSFER_DESCRIPTION,
                '{"fee": 0.0150704642, "coin": "DOT"}',
            ],
            [
                1,
                "2022-12-08 20:07:00",
                const.EARN_OPERATION,
                const.POLKADOT_EARN_DESCRIPTION,
                '{"amount": 0.1819792507, "coin": "DOT"}',
            ],
            [
                2,
                "2022-12-04 21:43:48",
                const.TRANSFER_OPERATION,
                const.POLKADOT_TRANSFER_DESCRIPTION,
                '{"fee": 0.0145475637, "coin": "DOT"}',
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
