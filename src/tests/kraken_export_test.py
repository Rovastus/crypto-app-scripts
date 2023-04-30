import unittest as ut
import pandas as pd
from kraken_export import KrakenExport
import constant as const


class KrakenExportTest(ut.TestCase):
    def test_deposit(self):
        test_df = pd.read_csv("tests/data/kraken/deposit.csv")
        export = KrakenExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 0, "Unexpected size")

    def test_staking(self):
        test_df = pd.read_csv("tests/data/kraken/staking.csv")
        export = KrakenExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 5, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-04-09 12:14:48",
                const.EARN_OPERATION,
                const.KRAKEN_EARN_DESCRIPTION,
                '{"amount": "1.556e-05", "coin": "SOL"}',
            ],
            [
                1,
                "2022-04-11 15:44:17",
                const.EARN_OPERATION,
                const.KRAKEN_EARN_DESCRIPTION,
                '{"amount": "1.1e-07", "coin": "BTC"}',
            ],
            [
                2,
                "2022-04-13 03:17:03",
                const.EARN_OPERATION,
                const.KRAKEN_EARN_DESCRIPTION,
                '{"amount": "0.00123985", "coin": "KSM"}',
            ],
            [
                3,
                "2022-04-13 07:47:57",
                const.EARN_OPERATION,
                const.KRAKEN_EARN_DESCRIPTION,
                '{"amount": "0.01132135", "coin": "DOT"}',
            ],
            [
                4,
                "2022-05-01 07:01:51",
                const.EARN_OPERATION,
                const.KRAKEN_EARN_DESCRIPTION,
                '{"amount": "5.7522e-05", "coin": "ETH2"}',
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

    def test_trade(self):
        test_df = pd.read_csv("tests/data/kraken/trade.csv")
        export = KrakenExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 6, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-04-08 22:15:54",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.00638978", "buyCoin": "BTC", "price": "249.9362", "priceCoin": "EUR", "fee": "0.6498", "feeCoin": "EUR"}',
            ],
            [
                1,
                "2022-04-08 22:16:22",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.08481131", "buyCoin": "ETH", "price": "249.966", "priceCoin": "EUR", "fee": "0.6499", "feeCoin": "EUR"}',
            ],
            [
                2,
                "2022-04-08 22:16:51",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "11.08033241", "buyCoin": "DOT", "price": "199.7175", "priceCoin": "EUR", "fee": "0.5193", "feeCoin": "EUR"}',
            ],
            [
                3,
                "2022-04-08 22:17:10",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.62893082", "buyCoin": "KSM", "price": "99.9182", "priceCoin": "EUR", "fee": "0.2598", "feeCoin": "EUR"}',
            ],
            [
                4,
                "2022-04-08 22:17:53",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "11.15075825", "buyCoin": "UNI", "price": "100.0", "priceCoin": "EUR", "fee": "0.16", "feeCoin": "EUR"}',
            ],
            [
                5,
                "2022-04-08 22:19:52",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.87933561", "buyCoin": "SOL", "price": "90.0", "priceCoin": "EUR", "fee": "0.144", "feeCoin": "EUR"}',
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

    def test_trade_no_price_exception(self):
        test_df = pd.read_csv("tests/data/kraken/trade_no_price.csv")
        export = KrakenExport()

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args[0],
            "No price row found.",
            "Exception arguments are not same",
        )

    def test_trade_no_buy_exception(self):
        test_df = pd.read_csv("tests/data/kraken/trade_no_buy.csv")
        export = KrakenExport()

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args[0],
            "No buy row found.",
            "Exception arguments are not same",
        )

    def test_trade_diff_times_exception(self):
        test_df = pd.read_csv("tests/data/kraken/trade_diff_times.csv")
        export = KrakenExport()

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args,
            (
                "UTC_Time values are not same.",
                ["2022-04-08 22:15:55", "2022-04-08 22:15:54"],
            ),
            "Exception arguments are not same",
        )

    def test_trade_diff_refid_exception(self):
        test_df = pd.read_csv("tests/data/kraken/trade_diff_refid.csv")
        export = KrakenExport()

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args,
            (
                "RefId values are not same.",
                ["THCN2S-4WUMZ-3GRCWS", "THCN2S-4WUMZ-3GRCWX"],
            ),
            "Exception arguments are not same",
        )

    def test_eth_staking(self):
        test_df = pd.read_csv("tests/data/kraken/transfer.csv")
        export = KrakenExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 9, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-04-08 22:23:00",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.08481", "buyCoin": "ETH2", "price": "0.08481", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                1,
                "2022-05-03 19:14:13",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.09469", "buyCoin": "ETH2", "price": "0.09469", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                2,
                "2022-06-02 21:01:03",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.14701", "buyCoin": "ETH2", "price": "0.14701", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                3,
                "2022-07-04 22:21:44",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.23216", "buyCoin": "ETH2", "price": "0.23216", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                4,
                "2022-08-02 18:20:16",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.2142", "buyCoin": "ETH2", "price": "0.2142", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                5,
                "2022-09-17 13:57:30",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.24574", "buyCoin": "ETH2", "price": "0.24574", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                6,
                "2022-10-04 15:12:28",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.25823", "buyCoin": "ETH2", "price": "0.25823", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                7,
                "2022-11-02 12:21:51",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.22413", "buyCoin": "ETH2", "price": "0.22413", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
            ],
            [
                8,
                "2022-12-03 22:30:39",
                const.TRANSACTION_OPERATION,
                const.KRAKEN_TRANSACTION_DESCRIPTION,
                '{"buy": "0.32883", "buyCoin": "ETH2", "price": "0.32883", "priceCoin": "ETH", "fee": "0", "feeCoin": "ETH"}',
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

    def test_withdrawal(self):
        test_df = pd.read_csv("tests/data/kraken/withdrawal.csv")
        export = KrakenExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 6, "Unexpected size")

        for test_case in [
            [
                0,
                "2022-12-04 19:24:48",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "1e-05", "coin": "BTC"}',
            ],
            [
                1,
                "2022-12-04 19:57:23",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "1e-05", "coin": "BTC"}',
            ],
            [
                2,
                "2022-12-04 20:15:23",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "0.05", "coin": "DOT"}',
            ],
            [
                3,
                "2022-12-04 20:27:09",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "0.05", "coin": "DOT"}',
            ],
            [
                4,
                "2022-12-06 22:15:10",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "0.01", "coin": "SOL"}',
            ],
            [
                5,
                "2022-12-15 19:52:35",
                const.TRANSFER_OPERATION,
                const.KRAKEN_TRANSFER_DESCRIPTION,
                '{"fee": "0.6", "coin": "UNI"}',
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
