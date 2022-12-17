import json
import pandas as pd

import constant as const


class Export:
    def __init__(self):
        self.new_df_data = []
        self.new_df_columns = const.CRYPTO_APP_EXPORT_COLUMNS

    def get_df(self):
        return pd.DataFrame(
            self.new_df_data,
            columns=self.new_df_columns,
        )

    def append_to_df(self, df):
        df.append(self.get_df())

    def read_export(self, df):
        i = 0
        while i < len(df.index):
            type = df[const.KRAKEN_TYPE_COLUMN][i]

            if type == const.KRAKEN_DEPOSIT_TYPE:
                pass
            elif type == const.KRAKEN_STAKING_TYPE:
                self.new_df_data.append(
                    [
                        df[const.KRAKEN_UTC_TIME_COLUMN][i],
                        const.EARN_OPERATION,
                        const.KRAKEN_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": df[const.KRAKEN_AMOUNT_COLUMN][i],
                                "coin": df[const.KRAKEN_ASSET_COLUMN][i],
                            }
                        ),
                    ]
                )
                pass
            elif type == const.KRAKEN_TRADE_TYPE:
                # load second column
                price = self.__get_price_row(df.iloc[i], df.iloc[i + 1])
                buy = self.__get_buy_row(df.iloc[i], df.iloc[i + 1])
                i += 1

                # check UTC times
                self.__check_times(
                    [
                        buy[const.KRAKEN_UTC_TIME_COLUMN],
                        price[const.KRAKEN_UTC_TIME_COLUMN],
                    ]
                )

                # check refid
                self.__check_refid(
                    [
                        buy[const.KRAKEN_REFID_COLUMN],
                        price[const.KRAKEN_REFID_COLUMN],
                    ]
                )

                # create new record
                self.new_df_data.append(
                    [
                        buy[const.KRAKEN_UTC_TIME_COLUMN],
                        const.TRANSACTION_OPERATION,
                        const.KRAKEN_TRANSACTION_DESCRIPTION,
                        json.dumps(
                            {
                                "buy": buy[const.KRAKEN_AMOUNT_COLUMN],
                                "buyCoin": buy[const.KRAKEN_ASSET_COLUMN],
                                "price": price[const.KRAKEN_AMOUNT_COLUMN],
                                "priceCoin": price[const.KRAKEN_ASSET_COLUMN],
                                "fee": price[const.KRAKEN_FEE_COLUMN],
                                "feeCoin": price[const.KRAKEN_FEE_COLUMN],
                            }
                        ),
                    ]
                )
            elif type == const.KRAKEN_TRANSFER_TYPE:
                if (
                    df[const.KRAKEN_SUBTYPE_COLUMN][i]
                    == const.KRAKEN_SPOT_TO_STAKING_SUBTYPE
                    and df[const.KRAKEN_ASSET_COLUMN][i] == "ETH"
                ):
                    # create new record
                    self.new_df_data.append(
                        [
                            df[const.KRAKEN_UTC_TIME_COLUMN][i],
                            const.TRANSACTION_OPERATION,
                            const.KRAKEN_TRANSACTION_DESCRIPTION,
                            json.dumps(
                                {
                                    "buy": abs(df[const.KRAKEN_AMOUNT_COLUMN][i]),
                                    "buyCoin": "ETH2",
                                    "price": df[const.KRAKEN_AMOUNT_COLUMN][i],
                                    "priceCoin": df[const.KRAKEN_ASSET_COLUMN][i],
                                    "fee": 0,
                                    "feeCoin": "ETH",
                                }
                            ),
                        ]
                    )
            elif type == const.KRAKEN_WITHDRAWAL_TYPE:
                if (
                    df[const.KRAKEN_FEE_COLUMN][i] > 0
                    and df[const.KRAKEN_BALANCE_COLUMN][i] is not None
                ):
                    # create new record
                    self.new_df_data.append(
                        [
                            df[const.KRAKEN_UTC_TIME_COLUMN][i],
                            const.TRANSFER_OPERATION,
                            const.KRAKEN_TRANSFER_DESCRIPTION,
                            json.dumps(
                                {
                                    "fee": df[const.KRAKEN_FEE_COLUMN][i],
                                    "feeCoin": df[const.KRAKEN_ASSET_COLUMN][i],
                                }
                            ),
                        ]
                    )
            else:
                raise Exception("Unexpected type.", type)

            i += 1

    def __check_times(self, time_list):
        utc_time = time_list[0]
        for compare_utc_time in time_list:
            if utc_time != compare_utc_time:
                raise Exception("UTC_Time values are not same.", time_list)

    def __check_refid(self, refid_list):
        refId = refid_list[0]
        for compare_refid in refid_list:
            if refId != compare_refid:
                raise Exception("RefId values are not same.", refid_list)

    def __get_buy_row(self, df_row_1, df_row_2):
        if df_row_1[const.KRAKEN_AMOUNT_COLUMN] >= 0:
            return df_row_1
        elif df_row_2[const.KRAKEN_AMOUNT_COLUMN] >= 0:
            return df_row_2
        else:
            raise Exception("No buy row found.", df_row_1, df_row_2)

    def __get_price_row(self, df_row_1, df_row_2):
        if df_row_1[const.KRAKEN_AMOUNT_COLUMN] < 0:
            return df_row_1
        elif df_row_2[const.KRAKEN_AMOUNT_COLUMN] < 0:
            return df_row_2
        else:
            raise Exception("No price row found.", df_row_1, df_row_2)
