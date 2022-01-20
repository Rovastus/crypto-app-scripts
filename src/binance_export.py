import constant as const
import json
import pandas as pd


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
            operation = df[const.OPERATION_COLUMN][i]

            if operation == const.DEPOSIT_OPERATION:
                deposit = df.iloc[i]

                # create new record
                self.new_df_data.append(
                    [
                        deposit[const.UTC_TIME_COLUMN],
                        const.DEPOSIT_OPERATION,
                        const.BINANCE_DEPOSIT_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": deposit[const.CHANGE_COLUMN],
                                "coin": deposit[const.COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif operation == const.WITHDRAWAL_OPERATION:
                withdrawal = df.iloc[i]

                # create new record
                self.new_df_data.append(
                    [
                        withdrawal[const.UTC_TIME_COLUMN],
                        const.WITHDRAWAL_OPERATION,
                        const.BINANCE_WITHDRAWAL_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": withdrawal[const.CHANGE_COLUMN],
                                "coin": withdrawal[const.COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif operation == const.BUY_OPERATION:
                # load buy, transaction related and fee rows
                buy = df.iloc[i]
                i += 1
                transaction_related = df.iloc[i]
                i += 1
                fee = df.iloc[i]

                # check operations
                self.__check_operation(buy[const.OPERATION_COLUMN], const.BUY_OPERATION)
                self.__check_operation(
                    transaction_related[const.OPERATION_COLUMN],
                    const.TRANSACTION_RELATED_OPERATION,
                )
                self.__check_operation(fee[const.OPERATION_COLUMN], const.FEE_OPERATION)

                # check UTC times
                self.__check_times(
                    [
                        buy[const.UTC_TIME_COLUMN],
                        transaction_related[const.UTC_TIME_COLUMN],
                        fee[const.UTC_TIME_COLUMN],
                    ]
                )

                # create new record
                self.new_df_data.append(
                    [
                        buy[const.UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.BINANCE_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": buy[const.CHANGE_COLUMN],
                                "buyCoin": buy[const.COIN_COLUMN],
                                "price": transaction_related[const.CHANGE_COLUMN],
                                "priceCoin": transaction_related[const.COIN_COLUMN],
                                "fee": fee[const.CHANGE_COLUMN],
                                "feeCoin": fee[const.COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif (
                operation == const.ETH_STAKING_TRANSACTION_OPERATION
                or operation == const.SMALL_ASSETS_EXCHANGE_BNB_OPERATION
                or operation == const.OTC_TRADING_OPERATION
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
                        buy[const.UTC_TIME_COLUMN],
                        price[const.UTC_TIME_COLUMN],
                    ]
                )

                # create new record
                self.new_df_data.append(
                    [
                        buy[const.UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.BINANCE_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": buy[const.CHANGE_COLUMN],
                                "buyCoin": buy[const.COIN_COLUMN],
                                "price": price[const.CHANGE_COLUMN],
                                "priceCoin": price[const.COIN_COLUMN],
                                "fee": 0,
                                "feeCoin": buy[const.COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif (
                operation == const.EARN_OPERATION
                or operation == const.POS_SAVINGS_INTEREST_OPERATION
                or operation == const.SAVINGS_INTEREST_OPERATION
                or operation == const.ETH_STAKING_REWARDS_OPERATION
                or operation == const.COMMISSION_FEE_OPERATION
                or operation == const.COMMISION_HISTORY_OPERATION
                or operation == const.REFERRAL_KICKBACK_OPERATION
            ):
                earn = df.iloc[i]

                # create new record
                self.new_df_data.append(
                    [
                        earn[const.UTC_TIME_COLUMN],
                        const.EARN_OPERATION,
                        const.BINANCE_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": earn[const.CHANGE_COLUMN],
                                "coin": earn[const.COIN_COLUMN],
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
        if df_row_1[const.CHANGE_COLUMN] >= 0:
            return df_row_1
        elif df_row_2[const.CHANGE_COLUMN] >= 0:
            return df_row_2
        else:
            raise Exception("No buy row found.", df_row_1, df_row_2)

    def __get_price_row(self, df_row_1, df_row_2):
        if df_row_1[const.CHANGE_COLUMN] < 0:
            return df_row_1
        elif df_row_2[const.CHANGE_COLUMN] < 0:
            return df_row_2
        else:
            raise Exception("No price row found.", df_row_1, df_row_2)
