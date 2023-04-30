import unittest as ut
import pandas as pd
from binance_earning_export import BinanceEarningExport
import constant as const


class BinanceEarningExportTest(ut.TestCase):
    def test_deposit(self):
        test_df = pd.read_csv("tests/data/binance/earning/deposit.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 0, "Unexpected size")

    def test_earn_ignored(self):
        test_df = pd.read_csv("tests/data/binance/earning/earn_ignored.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 0, "Unexpected size")

    def test_earn(self):
        test_df = pd.read_csv("tests/data/binance/earning/earn.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 9, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-01-01 00:54:09",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "0.0054699", "coin": "DOT"}',
            ],
            [
                1,
                "2022-01-01 03:17:32",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "0.00066963", "coin": "RUNE"}',
            ],
            [
                2,
                "2021-05-03 02:17:13",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "0.00011926", "coin": "BNB"}',
            ],
            [
                3,
                "2021-05-12 08:29:49",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "5.36e-06", "coin": "LTC"}',
            ],
            [
                4,
                "2021-05-30 08:26:20",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "6.71e-06", "coin": "BETH"}',
            ],
            [
                5,
                "2022-01-01 17:48:48",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "5.19e-06", "coin": "BTC"}',
            ],
            [
                6,
                "2022-01-19 21:57:27",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "8.63002252", "coin": "GLMR"}',
            ],
            [
                7,
                "2022-09-19 17:23:02",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "6.82e-05", "coin": "ETHW"}',
            ],
            [
                8,
                "2022-09-27 02:05:03",
                const.EARN_OPERATION,
                const.BINANCE_EARN_DESCRIPTION,
                '{"amount": "0.00318359", "coin": "DOT"}',
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

    def test_eth_staking(self):
        test_df = pd.read_csv("tests/data/binance/earning/eth_staking.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 1, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-05-13 23:11:32",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.BINANCE_TRANSACTION_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_TRANSACTION_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.BINANCE_DATA_COLUMN],
            '{"buy": "0.035", "buyCoin": "BETH", "price": "0.035", "priceCoin": "ETH", "fee": "0", "feeCoin": "BETH"}',
            "Unexpected data",
        )

    def test_transaction(self):
        test_df = pd.read_csv("tests/data/binance/earning/transaction.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 0, "Unexpected size")

    def test_withdrawal(self):
        test_df = pd.read_csv("tests/data/binance/earning/withdrawal.csv")
        export = BinanceEarningExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 0, "Unexpected size")

    def test_swap_exception(self):
        test_df = pd.read_csv("tests/data/binance/earning/eth_staking.csv")
        export = BinanceEarningExport()

        # test buy row not found
        test_df.loc[0, const.BINANCE_CHANGE_COLUMN] = -5

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args[0],
            "No buy row found.",
            "Exception argument are not same",
        )

        # test price row not found
        test_df = pd.read_csv("tests/data/binance/earning/eth_staking.csv")
        test_df.loc[1, const.BINANCE_CHANGE_COLUMN] = 5

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args[0],
            "No price row found.",
            "Exception argument are not same",
        )

        # test utc_time is not same
        test_df = pd.read_csv("tests/data/binance/earning/eth_staking.csv")
        test_df.loc[1, const.BINANCE_UTC_TIME_COLUMN] = "2021-05-13 23:11:33"

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args,
            (
                "UTC_Time values are not same.",
                ["2021-05-13 23:11:32", "2021-05-13 23:11:33"],
            ),
            "Exception arguments are not same",
        )
