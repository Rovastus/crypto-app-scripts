import json
import pandas as pd

import constant as const


class SolanaExport:
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
            type = df[const.SOLANA_TX_TYPE_COLUMN][i]

            if type == const.SOLANA_TRANSFER_TYPE:
                pass
            elif type == const.SOLANA_STAKING_TYPE:
                self.new_df_data.append(
                    [
                        df[const.SOLANA_UTC_TIME_COLUMN][i],
                        const.EARN_OPERATION,
                        const.SOLANA_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": df[const.SOLANA_RECEIVED_AMOUNT][i],
                                "coin": df[const.SOLANA_RECEIVED_CURRENCY_COLUMN][i],
                            }
                        ),
                    ]
                )
            elif type == const.SOLANA_SPEND_TYPE:
                # create new record
                self.new_df_data.append(
                    [
                        df[const.SOLANA_UTC_TIME_COLUMN][i],
                        const.TRANSFER_OPERATION,
                        const.SOLANA_SPEND_DESCRIPTION,
                        json.dumps(
                            {
                                "fee": df[const.SOLANA_SENT_AMOUNT_COLUMN][i],
                                "feeCoin": df[const.SOLANA_SENT_CURRENCY_COLUMN][i],
                            }
                        ),
                    ]
                )
            else:
                raise Exception("Unexpected type.", type)

            i += 1
