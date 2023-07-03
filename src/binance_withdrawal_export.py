import json

import pandas as pd

import constant as const


class BinanceWithdrawalExport:
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
            record = df.iloc[i]

            # create transfer new record
            self.new_df_data.append(
                [
                    record[const.BINANCE_WITHDRAWAL_UTC_TIME_COLUMM],
                    const.TRANSFER_OPERATION,
                    const.BINANCE_WITHDRAWAL_DESCRIPTION,
                    json.dumps(
                        {
                            "fee": str(
                                abs(
                                    record[
                                        const.BINANCE_WITHDRAWAL_TRANSACTION_FEE_COLUMM
                                    ]
                                )
                            ),
                            "coin": record[const.BINANCE_WITHDRAWAL_COIN_COLUMM],
                        }
                    ),
                ]
            )

            i += 1
