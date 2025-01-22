import json

import pandas as pd

import constant as const


class BinanceEarningExport:
    def __init__(self):
        self.new_df_data = []
        self.new_df_columns = const.CRYPTO_APP_EXPORT_COLUMNS

    def get_df(self):
        return pd.DataFrame(
            self.new_df_data,
            columns=self.new_df_columns,
        )

    def read_export(self, df):
        i = 0
        while i < len(df.index):
            operation = df[const.BINANCE_OPERATION_COLUMN][i]

            if operation in (
                const.BINANCE_DEPOSIT_OPERATION,
                const.BINANCE_FIAT_DEPOSIT_OPERATION,
                const.BINANCE_POS_SAVINGS_PURCHASE_OPERATION,
                const.BINANCE_POS_SAVINGS_REDEMPTION_OPERATION,
                const.BINANCE_SAVINGS_PURCHASE_OPERATION,
                const.BINANCE_SAVINGS_PRINCIPAL_REDEMPTION_OPERATION,
                const.BINANCE_SIMPLE_EARN_FLEXIBLE_SUBSCRIPTION_OPERATION,
                const.BINANCE_SIMPLE_EARN_FLEXIBLE_REDEMPTION_OPERATION,
                const.BINANCE_STAKING_PURCHASE_OPERATION,
                const.BINANCE_STAKING_REDEMPTION_OPERATION,
                const.BINANCE_SIMPLE_EARN_LOCKED_SUBSCRIPTION_OPERATION,
                const.BINANCE_SIMPLE_EARN_LOCKED_REDEMPTION_OPERATION,
                const.BINANCE_DOT_REDEMPTION_OPERATION
            ):
                pass
            elif operation == const.BINANCE_WITHDRAWAL_OPERATION:
                # can skip this operation as withdrawals export is used for this operation
                pass
            elif operation in (
                const.BINANCE_BUY_OPERATION,
                const.BINANCE_TRANSACTION_RELATED_OPERATION,
                const.BINANCE_FEE_OPERATION,
            ):
                # can skip those operations as transaction export is used for those operation
                pass
            elif operation in (
                const.BINANCE_ETH_STAKING_TRANSACTION_OPERATION,
                const.BINANCE_ETH_STAKING_WITHDRAWALS_TRANSACTION_OPERATION,
                const.BINANCE_ASSET_RECOVERY_TRANSACTION_OPERATION
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

                # create new transaction record
                self.new_df_data.append(
                    [
                        buy[const.BINANCE_UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.BINANCE_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": str(abs(buy[const.BINANCE_CHANGE_COLUMN])),
                                "buyCoin": buy[const.BINANCE_COIN_COLUMN],
                                "price": str(abs(price[const.BINANCE_CHANGE_COLUMN])),
                                "priceCoin": price[const.BINANCE_COIN_COLUMN],
                                "fee": "0",
                                "feeCoin": buy[const.BINANCE_COIN_COLUMN],
                            }
                        ),
                    ]
                )
            elif operation in (
                const.BINANCE_STAKING_REWARDS_OPERATION,
                const.BINANCE_SIMPLE_EARN_FLEXIBLE_INTEREST_OPERATION,
                const.BINANCE_POS_SAVINGS_INTEREST_OPERATION,
                const.BINANCE_SAVINGS_INTEREST_OPERATION,
                const.BINANCE_ETH_STAKING_REWARDS_OPERATION,
                const.BINANCE_SAVINGS_DISTRIBUTION_OPERATION,
                const.BINANCE_DOT_SLOT_AUCTION_REWARDS,
                const.BINANCE_DISTRIBUTION_OPERATION,
                const.BINANCE_SIMPLE_EARN_LOCKED_REWARDS_OPERATION,
                const.BINANCE_LAUNCHPOOL_WITHDRAWAL_OPERATION
            ):
                earn = df.iloc[i]
                # remove LD from coin value when operation is const.BINANCE_SAVINGS_DISTRIBUTION_OPERATION
                coin = (
                    earn[const.BINANCE_COIN_COLUMN][2:]
                    if operation == const.BINANCE_SAVINGS_DISTRIBUTION_OPERATION
                    else earn[const.BINANCE_COIN_COLUMN]
                )

                # create new earn record
                self.new_df_data.append(
                    [
                        earn[const.BINANCE_UTC_TIME_COLUMN],
                        const.EARN_OPERATION,
                        const.BINANCE_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": str(abs(earn[const.BINANCE_CHANGE_COLUMN])),
                                "coin": coin,
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

    def __get_buy_row(self, df_row_1, df_row_2):
        if df_row_1[const.BINANCE_CHANGE_COLUMN] >= 0:
            return df_row_1
        if df_row_2[const.BINANCE_CHANGE_COLUMN] >= 0:
            return df_row_2

        raise Exception("No buy row found.", df_row_1, df_row_2)

    def __get_price_row(self, df_row_1, df_row_2):
        if df_row_1[const.BINANCE_CHANGE_COLUMN] < 0:
            return df_row_1
        if df_row_2[const.BINANCE_CHANGE_COLUMN] < 0:
            return df_row_2

        raise Exception("No price row found.", df_row_1, df_row_2)
