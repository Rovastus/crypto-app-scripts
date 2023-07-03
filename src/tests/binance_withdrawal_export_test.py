import unittest as ut

import pandas as pd

import constant as const
from binance_withdrawal_export import BinanceWithdrawalExport


class BinanceWithdrawalExportTest(ut.TestCase):
    def test_transaction(self):
        test_df = pd.read_csv("tests/data/binance/withdrawal/withdrawal.csv")
        export = BinanceWithdrawalExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 6, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-12-16 22:01:45",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.0002", "coin": "BTC"}',
            ],
            [
                1,
                "2022-12-15 18:59:55",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.53", "coin": "UNI"}',
            ],
            [
                2,
                "2022-12-15 18:50:13",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.53", "coin": "UNI"}',
            ],
            [
                3,
                "2022-12-08 19:57:31",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.08", "coin": "DOT"}',
            ],
            [
                4,
                "2022-11-20 19:46:27",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.0002", "coin": "BTC"}',
            ],
            [
                5,
                "2022-11-20 19:26:47",
                const.TRANSFER_OPERATION,
                const.BINANCE_WITHDRAWAL_DESCRIPTION,
                '{"fee": "0.0002", "coin": "BTC"}',
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
