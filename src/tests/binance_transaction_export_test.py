import unittest as ut
import pandas as pd
from binance_transaction_export import BinanceTransactionExport
import constant as const


class BinanceTransactionExportTest(ut.TestCase):
    def test_transaction(self):
        test_df = pd.read_csv("tests/data/binance/transaction/transaction.csv")
        export = BinanceTransactionExport()
        export.read_export(test_df)
        result_df = export.get_df()

        self.assertEqual(len(result_df), 6, "Unexpected size")

        for test_case in [
            [
                0,
                "2021-12-01 09:32:17",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "0.176", "buyCoin": "BNB", "price": "99.44", "priceCoin": "EUR", "fee": "0.000132", "feeCoin": "BNB"}',
            ],
            [
                1,
                "2021-12-01 09:32:08",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "5.23", "buyCoin": "UNI", "price": "99.9453", "priceCoin": "EUR", "fee": "0.00523", "feeCoin": "UNI"}',
            ],
            [
                2,
                "2021-12-01 09:31:23",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "0.0239", "buyCoin": "ETH", "price": "99.663", "priceCoin": "EUR", "fee": "2.39e-05", "feeCoin": "ETH"}',
            ],
            [
                3,
                "2021-09-16 20:22:49",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "170.95", "buyCoin": "USDT", "price": "2.63", "priceCoin": "AVAX", "fee": "0.0003036", "feeCoin": "BNB"}',
            ],
            [
                4,
                "2021-09-05 10:36:29",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "401.2", "buyCoin": "USDT", "price": "236.0", "priceCoin": "MATIC", "fee": "0.4012", "feeCoin": "USDT"}',
            ],
            [
                5,
                "2021-04-30 13:03:03",
                const.TRANSACTION_OPERATION,
                const.BINANCE_TRANSACTION_DESCRIPTION,
                '{"buy": "1.647", "buyCoin": "DOT", "price": "49.0806", "priceCoin": "EUR", "fee": "7.273e-05", "feeCoin": "BNB"}',
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

    def test_transaction_exception(self):
        test_df = pd.read_csv(
            "tests/data/binance/transaction/transaction_unknown_side.csv"
        )
        export = BinanceTransactionExport()

        with self.assertRaises(Exception) as context:
            export.read_export(test_df)
        self.assertEqual(
            context.exception.args,
            (
                "Unknown side value.",
                "UNKNOWN",
            ),
            "Exception arguments are not same",
        )
