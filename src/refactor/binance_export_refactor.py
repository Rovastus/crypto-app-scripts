import requests
import numpy as np

import constant as const

SELL_OPERATION = "Sell"

TRANSACTION_UTC_TIME_COLUMN = "Date(UTC)"
TRANSACTION_PAIR_COLUMN = "Pair"
TRANSACTION_SIDE_COLUMN = "Side"
TRANSACTION_EXECUTIED_COLUMN = "Executed"
TRANSACTION_AMOUNT_COLUMN = "Amount"
TRANSACTION_FEE_COLUMN = "Fee"

TRANSACTION_FEE_BNB = "00BNB"
TRANSACTION_PAIR_BNB = "BNB"


class BinanceExportRefactor:
    def __init__(self):
        self.pairs = {}
        for record in requests.get(
            "https://api.binance.com/api/v1/exchangeInfo"
        ).json()["symbols"]:
            self.pairs[record["symbol"]] = [record["baseAsset"], record["quoteAsset"]]

    def refactor(self, export, transactions):
        # update Sell -> Buy
        export[const.BINANCE_OPERATION_COLUMN] = np.where(
            export[const.BINANCE_OPERATION_COLUMN] == SELL_OPERATION,
            const.BINANCE_BUY_OPERATION,
            export[const.BINANCE_OPERATION_COLUMN],
        )

        # update Buy -> Transaction Related if change is lower than 0
        export[const.BINANCE_OPERATION_COLUMN] = np.where(
            (export[const.BINANCE_OPERATION_COLUMN] == const.BINANCE_BUY_OPERATION)
            & (export[const.BINANCE_CHANGE_COLUMN] < 0),
            const.BINANCE_TRANSACTION_RELATED_OPERATION,
            export[const.BINANCE_OPERATION_COLUMN],
        )

        export_copy = export.copy()
        indexes_temp = []
        indexes_sorted_temp = []
        time_temp = ""

        # sort buy, transaction related, fee operations
        for i in export.index:
            if (
                time_temp != export[const.BINANCE_UTC_TIME_COLUMN][i]
                or i == export.index.stop - 1
            ):
                if i == export.index.stop - 1:
                    if (
                        export[const.BINANCE_OPERATION_COLUMN][i]
                        == const.BINANCE_BUY_OPERATION
                        or export[const.BINANCE_OPERATION_COLUMN][i]
                        == const.BINANCE_TRANSACTION_RELATED_OPERATION
                        or export[const.BINANCE_OPERATION_COLUMN][i]
                        == const.BINANCE_FEE_OPERATION
                    ):
                        indexes_temp.append(i)

                if len(indexes_temp) > 0:
                    # sort indexes
                    if len(indexes_temp) == 3:
                        indexes_sorted_temp.append(
                            self.__get_index_by_operation(
                                export, indexes_temp, const.BINANCE_BUY_OPERATION
                            )
                        )
                        indexes_sorted_temp.append(
                            self.__get_index_by_operation(
                                export,
                                indexes_temp,
                                const.BINANCE_TRANSACTION_RELATED_OPERATION,
                            )
                        )
                        indexes_sorted_temp.append(
                            self.__get_index_by_operation(
                                export, indexes_temp, const.BINANCE_FEE_OPERATION
                            )
                        )
                    else:
                        filtered_transaction = transactions.loc[
                            transactions[TRANSACTION_UTC_TIME_COLUMN] == time_temp
                        ]
                        for j in filtered_transaction.index:
                            pair_base = self.pairs[
                                filtered_transaction[TRANSACTION_PAIR_COLUMN][j]
                            ][0]
                            pair_quote = self.pairs[
                                filtered_transaction[TRANSACTION_PAIR_COLUMN][j]
                            ][1]
                            if (
                                filtered_transaction[TRANSACTION_SIDE_COLUMN][j]
                                == "BUY"
                            ):
                                buy_price = filtered_transaction[
                                    TRANSACTION_EXECUTIED_COLUMN
                                ][j].replace(pair_base, "")
                                buy_coin = pair_base
                                transaction_related_price = filtered_transaction[
                                    TRANSACTION_AMOUNT_COLUMN
                                ][j].replace(pair_quote, "")
                                transaction_related_coin = pair_quote
                                fee_price = (
                                    filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].endswith(TRANSACTION_FEE_BNB)
                                    and filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].replace(TRANSACTION_PAIR_BNB, "")
                                    or filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].replace(pair_base, "")
                                )
                                fee_coin = (
                                    filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].endswith(TRANSACTION_FEE_BNB)
                                    and TRANSACTION_PAIR_BNB
                                    or pair_base
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_BUY_OPERATION,
                                        buy_price,
                                        buy_coin,
                                    )
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_TRANSACTION_RELATED_OPERATION,
                                        transaction_related_price,
                                        transaction_related_coin,
                                    )
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_FEE_OPERATION,
                                        fee_price,
                                        fee_coin,
                                    )
                                )
                            elif (
                                filtered_transaction[TRANSACTION_SIDE_COLUMN][j]
                                == "SELL"
                            ):
                                buy_price = filtered_transaction[
                                    TRANSACTION_AMOUNT_COLUMN
                                ][j].replace(pair_quote, "")
                                buy_coin = pair_quote
                                transaction_related_price = filtered_transaction[
                                    TRANSACTION_EXECUTIED_COLUMN
                                ][j].replace(pair_base, "")
                                transaction_related_coin = pair_base
                                fee_price = (
                                    filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].endswith(TRANSACTION_FEE_BNB)
                                    and filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].replace(TRANSACTION_PAIR_BNB, "")
                                    or filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].replace(pair_quote, "")
                                )
                                fee_coin = (
                                    filtered_transaction[TRANSACTION_FEE_COLUMN][
                                        j
                                    ].endswith(TRANSACTION_FEE_BNB)
                                    and TRANSACTION_PAIR_BNB
                                    or pair_quote
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_BUY_OPERATION,
                                        buy_price,
                                        buy_coin,
                                    )
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_TRANSACTION_RELATED_OPERATION,
                                        transaction_related_price,
                                        transaction_related_coin,
                                    )
                                )
                                indexes_sorted_temp.append(
                                    self.__get_index_by_operation_and_price(
                                        export,
                                        indexes_temp,
                                        const.BINANCE_FEE_OPERATION,
                                        fee_price,
                                        fee_coin,
                                    )
                                )
                            else:
                                raise Exception(
                                    j, filtered_transaction[TRANSACTION_SIDE_COLUMN][j]
                                )

                    print(indexes_temp)
                    print(indexes_sorted_temp)
                    for idx, value in enumerate(indexes_temp):
                        export.iloc[value] = export_copy.iloc[indexes_sorted_temp[idx]]

                    indexes_temp = []
                    indexes_sorted_temp = []

            time_temp = export[const.BINANCE_UTC_TIME_COLUMN][i]

            if (
                export[const.BINANCE_OPERATION_COLUMN][i] == const.BINANCE_BUY_OPERATION
                or export[const.BINANCE_OPERATION_COLUMN][i]
                == const.BINANCE_TRANSACTION_RELATED_OPERATION
                or export[const.BINANCE_OPERATION_COLUMN][i]
                == const.BINANCE_FEE_OPERATION
            ):
                indexes_temp.append(i)

    def __get_index_by_operation(self, df, indexes, operation):
        for i in indexes:
            if df[const.BINANCE_OPERATION_COLUMN][i] == operation:
                return i
        raise Exception(indexes, operation)

    def __get_index_by_operation_and_price(self, df, indexes, operation, price, coin):
        price_float = float(price)
        if (
            operation == const.BINANCE_TRANSACTION_RELATED_OPERATION
            or operation == const.BINANCE_FEE_OPERATION
        ):
            price_float = price_float * -1

        for i in indexes:
            if (
                df[const.BINANCE_OPERATION_COLUMN][i] == operation
                and float(df[const.BINANCE_CHANGE_COLUMN][i]) == price_float
                and df[const.BINANCE_COIN_COLUMN][i] == coin
            ):
                return i
        raise Exception(indexes, operation, price, coin)
