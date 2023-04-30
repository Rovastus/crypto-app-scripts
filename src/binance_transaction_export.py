import json
import pandas as pd
import requests
import constant as const


class BinanceTransactionExport:
    def __init__(self):
        self.new_df_data = []
        self.new_df_columns = const.CRYPTO_APP_EXPORT_COLUMNS
        self.pairs = {}
        for record in requests.get(
            "https://api.binance.com/api/v1/exchangeInfo", timeout=1000
        ).json()["symbols"]:
            self.pairs[record["symbol"]] = [record["baseAsset"], record["quoteAsset"]]

    def get_df(self):
        return pd.DataFrame(
            self.new_df_data,
            columns=self.new_df_columns,
        )

    def read_export(self, df):
        i = 0

        while i < len(df.index):
            record = df.iloc[i]

            buy = None
            price = None
            side = record[const.BINANCE_TRANSACTION_SIDE_COLUMM]

            if side == const.BINANCE_TRANSACTION_SIDE_BUY_VALUE:
                buy = self.__get_value_and_pair(
                    record[const.BINANCE_TRANSACTION_EXECUTED_COLUMM],
                    record[const.BINANCE_TRANSACTION_PAIR_COLUMM],
                )
                price = self.__get_value_and_pair(
                    record[const.BINANCE_TRANSACTION_AMOUNT_COLUMM],
                    record[const.BINANCE_TRANSACTION_PAIR_COLUMM],
                )
            elif side == const.BINANCE_TRANSACTION_SIDE_SELL_VALUE:
                buy = self.__get_value_and_pair(
                    record[const.BINANCE_TRANSACTION_AMOUNT_COLUMM],
                    record[const.BINANCE_TRANSACTION_PAIR_COLUMM],
                )
                price = self.__get_value_and_pair(
                    record[const.BINANCE_TRANSACTION_EXECUTED_COLUMM],
                    record[const.BINANCE_TRANSACTION_PAIR_COLUMM],
                )
            else:
                raise Exception("Unknown side value.", side)

            fee = self.__get_value_and_pair(
                record[const.BINANCE_TRANSACTION_FEE_COLUMM],
                record[const.BINANCE_TRANSACTION_PAIR_COLUMM],
            )

            # create transaction new record
            self.new_df_data.append(
                [
                    record[const.BINANCE_TRANSACTION_UTC_TIME_COLUMM],
                    const.TRANSACTION_OPERATION,
                    const.BINANCE_TRANSACTION_DESCRIPTION,
                    json.dumps(
                        {
                            "buy": str(abs(buy[0])),
                            "buyCoin": buy[1],
                            "price": str(abs(price[0])),
                            "priceCoin": price[1],
                            "fee": str(abs(fee[0])),
                            "feeCoin": fee[1],
                        }
                    ),
                ]
            )

            i += 1

    def __get_value_and_pair(self, record, pair):
        if record.endswith(self.pairs[pair][0]):
            return [float(record.replace(self.pairs[pair][0], "")), self.pairs[pair][0]]
        if record.endswith(self.pairs[pair][1]):
            return [float(record.replace(self.pairs[pair][1], "")), self.pairs[pair][1]]
        if record.endswith("BNB"):
            return [float(record.replace("BNB", "")), "BNB"]

        raise Exception("Invalid record.", record, pair)
