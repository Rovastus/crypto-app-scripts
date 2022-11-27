import json
import pandas as pd

import src.constant as const


class Export:
    def __init__(self):
        self.new_df_data = []
        self.new_df_columns = [
            const.UTC_TIME_COLUMN,
            const.OPERATION_COLUMN,
            const.DESCRIPTION_COLUMN,
            const.DATA_COLUMN,
        ]

    def get_df(self):
        return pd.DataFrame(
            self.new_df_data,
            columns=self.new_df_columns,
        )

    def read_export(self, df):
        i = 0
        while i < len(df.index):
            operation = df[const.BINANCE_OPERATION_COLUMN][i]

            if operation == const.BINANCE_DEPOSIT_OPERATION:
                pass
            elif operation == const.BINANCE_WITHDRAWAL_OPERATION:
                # TODO: create Transfer operation
                pass
            elif operation == const.BINANCE_BUY_OPERATION:
                # load buy, transaction related and fee rows
                buy = df.iloc[i]
                i += 1
                transaction_related = df.iloc[i]
                i += 1
                fee = df.iloc[i]

                # check operations
                self.__check_operation(
                    buy[const.BINANCE_OPERATION_COLUMN], const.BINANCE_BUY_OPERATION
                )
                self.__check_operation(
                    transaction_related[const.BINANCE_OPERATION_COLUMN],
                    const.BINANCE_TRANSACTION_RELATED_OPERATION,
                )
                self.__check_operation(
                    fee[const.BINANCE_OPERATION_COLUMN], const.BINANCE_FEE_OPERATION
                )

                # check UTC times
                self.__check_times(
                    [
                        buy[const.BINANCE_UTC_TIME_COLUMN],
                        transaction_related[const.BINANCE_UTC_TIME_COLUMN],
                        fee[const.BINANCE_UTC_TIME_COLUMN],
                    ]
                )

                # create new record
                self.new_df_data.append(
                    [
                        buy[const.BINANCE_UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.BINANCE_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": buy[const.BINANCE_CHANGE_COLUMN],
                                "buyCoin": buy[const.BINANCE_COIN_COLUMN],
                                "price": transaction_related[
                                    const.BINANCE_CHANGE_COLUMN
                                ],
                                "priceCoin": transaction_related[
                                    const.BINANCE_COIN_COLUMN
                                ],
                                "fee": fee[const.BINANCE_CHANGE_COLUMN],
                                "feeCoin": fee[const.BINANCE_COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif (
                operation == const.BINANCE_ETH_STAKING_TRANSACTION_OPERATION
                or operation == const.BINANCE_SMALL_ASSETS_EXCHANGE_BNB_OPERATION
                or operation == const.BINANCE_OTC_TRADING_OPERATION
            ):
                # load two rows
                row_1 = df.iloc[i]
                i += 1
                row_2 = df.iloc[i]

                # check operations
                buy = self.__get_buy_row(row_1, row_2)
                price = self.__get_price_row(row_1, row_2)

                # check UTC times
                self.__check_times(
                    [
                        buy[const.BINANCE_UTC_TIME_COLUMN],
                        price[const.BINANCE_UTC_TIME_COLUMN],
                    ]
                )

                # create new record
                self.new_df_data.append(
                    [
                        buy[const.BINANCE_UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.BINANCE_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": buy[const.BINANCE_CHANGE_COLUMN],
                                "buyCoin": buy[const.BINANCE_COIN_COLUMN],
                                "price": price[const.BINANCE_CHANGE_COLUMN],
                                "priceCoin": price[const.BINANCE_COIN_COLUMN],
                                "fee": 0,
                                "feeCoin": buy[const.BINANCE_COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif (
                operation == const.BINANCE_EARN_OPERATION
                or operation == const.BINANCE_POS_SAVINGS_INTEREST_OPERATION
                or operation == const.BINANCE_SAVINGS_INTEREST_OPERATION
                or operation == const.BINANCE_ETH_STAKING_REWARDS_OPERATION
                or operation == const.BINANCE_COMMISSION_FEE_OPERATION
                or operation == const.BINANCE_COMMISION_HISTORY_OPERATION
                or operation == const.BINANCE_REFERRAL_KICKBACK_OPERATION
            ):
                earn = df.iloc[i]

                # create new record
                self.new_df_data.append(
                    [
                        earn[const.BINANCE_UTC_TIME_COLUMN],
                        const.EARN_OPERATION,
                        const.BINANCE_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": earn[const.BINANCE_CHANGE_COLUMN],
                                "coin": earn[const.BINANCE_COIN_COLUMN],
                            }
                        ),
                    ]
                )
            else:
                raise Exception("Unexpected operation.", operation)

            i += 1

    def __check_times(self, time_list):
        utc_time = time_list[0]
        for compare_utc_time in time_list:
            if utc_time != compare_utc_time:
                raise Exception("UTC_Time values are not same.", time_list)

    def __check_operation(self, operation, expected_operation):
        if operation != expected_operation:
            raise Exception("Unexpected operation.", operation, expected_operation)

    def __get_buy_row(self, df_row_1, df_row_2):
        if df_row_1[const.BINANCE_CHANGE_COLUMN] >= 0:
            return df_row_1
        elif df_row_2[const.BINANCE_CHANGE_COLUMN] >= 0:
            return df_row_2
        else:
            raise Exception("No buy row found.", df_row_1, df_row_2)

    def __get_price_row(self, df_row_1, df_row_2):
        if df_row_1[const.BINANCE_CHANGE_COLUMN] < 0:
            return df_row_1
        elif df_row_2[const.BINANCE_CHANGE_COLUMN] < 0:
            return df_row_2
        else:
            raise Exception("No price row found.", df_row_1, df_row_2)
