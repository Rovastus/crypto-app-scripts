import unittest as ut
import binance_export as ex
import pandas as pd
import constant as const


class BinanceExportTests(ut.TestCase):
    def test_deposit(self):
        test_df = pd.read_csv("src/test/deposit.csv")
        export = ex.Export()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 1, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-04-29 20:03:43",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.DEPOSIT_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_DEPOSIT_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 98.2, "coin": "EUR"}',
            "Unexpected data",
        )

    def test_earn(self):
        test_df = pd.read_csv("src/test/earn.csv")
        export = ex.Export()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 6, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-05-03 02:17:13",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 0.00011926, "coin": "BNB"}',
            "Unexpected data",
        )

        result_row = result_df.iloc[1]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-05-12 08:29:49",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 5.36e-06, "coin": "LTC"}',
            "Unexpected data",
        )

        result_row = result_df.iloc[2]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-05-30 08:26:20",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 6.71e-06, "coin": "BETH"}',
            "Unexpected data",
        )

        result_row = result_df.iloc[3]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-03-20 18:46:34",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 9.02e-06, "coin": "BNB"}',
            "Unexpected data",
        )

        result_row = result_df.iloc[4]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-03-25 15:19:13",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 0.00359, "coin": "ADA"}',
            "Unexpected data",
        )

        result_row = result_df.iloc[5]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-10-07 16:55:46",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.EARN_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_EARN_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"amount": 2.04e-05, "coin": "BNB"}',
            "Unexpected data",
        )

    def test_eth_staking(self):
        test_df = pd.read_csv("src/test/eth_staking.csv")
        export = ex.Export()
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
            const.TRANSACTION_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_TRANSACTION_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"buy": 0.035, "buyCoin": "BETH", "price": -0.035, "priceCoin": "ETH", "fee": 0, "feeCoin": "BETH"}',
            "Unexpected data",
        )

    def test_otc_trading(self):
        test_df = pd.read_csv("src/test/otc_trading.csv")
        export = ex.Export()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 1, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-03-19 19:25:25",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.TRANSACTION_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_TRANSACTION_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"buy": 0.0072063, "buyCoin": "ETH", "price": -10.0, "priceCoin": "ADA", "fee": 0, "feeCoin": "ETH"}',
            "Unexpected data",
        )

    def test_small_asset_exchange(self):
        test_df = pd.read_csv("src/test/small_asset_exchange.csv")
        export = ex.Export()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 1, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-03-19 16:52:50",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.TRANSACTION_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_TRANSACTION_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"buy": 0.00069718, "buyCoin": "BNB", "price": -0.189541, "priceCoin": "USDT", "fee": 0, "feeCoin": "BNB"}',
            "Unexpected data",
        )

    def test_transaction(self):
        test_df = pd.read_csv("src/test/transaction.csv")
        export = ex.Export()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 1, "Unexpected size")

        result_row = result_df.iloc[0]
        self.assertEqual(
            result_row[const.UTC_TIME_COLUMN],
            "2021-04-30 10:58:56",
            "Unexpected utc_time",
        )
        self.assertEqual(
            result_row[const.OPERATION_COLUMN],
            const.TRANSACTION_OPERATION,
            "Unexpected operation",
        )
        self.assertEqual(
            result_row[const.DESCRIPTION_COLUMN],
            const.BINANCE_TRANSACTION_DESCRIPTION,
            "Unexpected description",
        )
        self.assertEqual(
            result_row[const.DATA_COLUMN],
            '{"buy": 0.0982, "buyCoin": "BNB", "price": -49.1, "priceCoin": "EUR", "fee": -7.365e-05, "feeCoin": "BNB"}',
            "Unexpected data",
        )
