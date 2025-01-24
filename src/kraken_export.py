import json

import numpy as np
import pandas as pd

import constant as const


class KrakenExport:
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
            type_value = df[const.KRAKEN_TYPE_COLUMN][i]

            if type_value == const.KRAKEN_DEPOSIT_TYPE:
                pass
            elif type_value in (
                const.KRAKEN_STAKING_TYPE,
                const.KRAKEN_EARN_TYPE
            ) and df[const.KRAKEN_SUBTYPE_COLUMN][i] != const.KRAKEN_MIGRATION_SUBTYPE:
                self.new_df_data.append(
                    [
                        df[const.KRAKEN_UTC_TIME_COLUMN][i],
                        const.EARN_OPERATION,
                        const.KRAKEN_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": str(abs(df[const.KRAKEN_AMOUNT_COLUMN][i])),
                                "fee": str(abs(df[const.KRAKEN_FEE_COLUMN][i])),
                                "coin": df[const.KRAKEN_ASSET_COLUMN][i],
                            }
                        ),
                    ]
                )
            elif type_value == const.KRAKEN_EARN_TYPE and df[const.KRAKEN_SUBTYPE_COLUMN][i] == const.KRAKEN_MIGRATION_SUBTYPE and df[const.KRAKEN_ASSET_COLUMN][i] not in ("ETH", "ETH2"):
                pass
            elif type_value == const.KRAKEN_TRADE_TYPE or (type_value == const.KRAKEN_EARN_TYPE and df[const.KRAKEN_SUBTYPE_COLUMN][i] == const.KRAKEN_MIGRATION_SUBTYPE):
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
                                "buy": str(abs(buy[const.KRAKEN_AMOUNT_COLUMN])),
                                "buyCoin": buy[const.KRAKEN_ASSET_COLUMN],
                                "price": str(abs(price[const.KRAKEN_AMOUNT_COLUMN])),
                                "priceCoin": price[const.KRAKEN_ASSET_COLUMN],
                                "fee": str(abs(price[const.KRAKEN_FEE_COLUMN])),
                                "feeCoin": price[const.KRAKEN_ASSET_COLUMN],
                            }
                        ),
                    ]
                )
            elif type_value == const.KRAKEN_TRANSFER_TYPE:
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
                                    "buy": str(abs(df[const.KRAKEN_AMOUNT_COLUMN][i])),
                                    "buyCoin": "ETH2",
                                    "price": str(
                                        abs(df[const.KRAKEN_AMOUNT_COLUMN][i])
                                    ),
                                    "priceCoin": df[const.KRAKEN_ASSET_COLUMN][i],
                                    "fee": "0",
                                    "feeCoin": "ETH",
                                }
                            ),
                        ]
                    )
            elif type_value == const.KRAKEN_WITHDRAWAL_TYPE:
                if df[const.KRAKEN_TXID_COLUMN][i] is not np.nan:
                    # create new record
                    self.new_df_data.append(
                        [
                            df[const.KRAKEN_UTC_TIME_COLUMN][i],
                            const.TRANSFER_OPERATION,
                            const.KRAKEN_TRANSFER_DESCRIPTION,
                            json.dumps(
                                {
                                    "fee": str(abs(df[const.KRAKEN_FEE_COLUMN][i])),
                                    "coin": df[const.KRAKEN_ASSET_COLUMN][i],
                                }
                            ),
                        ]
                    )
            else:
                raise Exception("Unexpected type.", type_value)

            i += 1

    def __check_times(self, time_list):
        utc_time = time_list[0]
        for compare_utc_time in time_list:
            if utc_time != compare_utc_time:
                raise Exception("UTC_Time values are not same.", time_list)

    def __check_refid(self, refid_list):
        ref_id = refid_list[0]
        for compare_refid in refid_list:
            if ref_id != compare_refid:
                raise Exception("RefId values are not same.", refid_list)

    def __get_buy_row(self, df_row_1, df_row_2):
        if df_row_1[const.KRAKEN_AMOUNT_COLUMN] >= 0:
            return df_row_1
        if df_row_2[const.KRAKEN_AMOUNT_COLUMN] >= 0:
            return df_row_2

        raise Exception("No buy row found.", df_row_1, df_row_2)

    def __get_price_row(self, df_row_1, df_row_2):
        if df_row_1[const.KRAKEN_AMOUNT_COLUMN] < 0:
            return df_row_1
        if df_row_2[const.KRAKEN_AMOUNT_COLUMN] < 0:
            return df_row_2

        raise Exception("No price row found.", df_row_1, df_row_2)
