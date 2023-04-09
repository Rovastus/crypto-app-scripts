import json
import time
import decimal
import pandas as pd
import requests
import constant as const


class PolkadotExport:
    def __init__(self):
        self.extrinsic_url = "https://polkadot.api.subscan.io/api/scan/extrinsic"
        self.transactions_url = "https://polkadot.api.subscan.io/api/v2/scan/transfers"
        self.headers = {"Content-Type": "application/json"}
        self.new_df_data = []
        self.new_df_columns = const.CRYPTO_APP_EXPORT_COLUMNS
        self.call_counter = 0
        decimal.getcontext().prec = 10

    def get_df(self):
        return pd.DataFrame(
            self.new_df_data,
            columns=self.new_df_columns,
        )

    def read_export(self, df):
        i = 0
        while i < len(df.index):
            action = df[const.POLKADOT_ACTION_COLUMN][i]

            if action == const.POLKADOT_JOIN_NOMINATION_POOLS_ACTION:
                # create transfer record
                self.new_df_data.append(
                    [
                        df[const.POLKADOT_UTC_TIME_COLUMN][i],
                        const.TRANSFER_OPERATION,
                        const.POLKADOT_TRANSFER_DESCRIPTION,
                        json.dumps(
                            {
                                "fee": float(
                                    self.__get_fee(
                                        df[const.POLKADOT_EXTRINSIC_ID_COLUMN][i]
                                    ).copy_abs()
                                ),
                                "coin": "DOT",
                            }
                        ),
                    ]
                )
            elif action == const.POLKADOT_BOND_EXTRA_NOMINATION_POOLS_ACTION:
                # create transfer record
                self.new_df_data.append(
                    [
                        df[const.POLKADOT_UTC_TIME_COLUMN][i],
                        const.TRANSFER_OPERATION,
                        const.POLKADOT_TRANSFER_DESCRIPTION,
                        json.dumps(
                            {
                                "fee": float(
                                    self.__get_fee(
                                        df[const.POLKADOT_EXTRINSIC_ID_COLUMN][i]
                                    ).copy_abs()
                                ),
                                "coin": "DOT",
                            }
                        ),
                    ]
                )

                # create earn record
                self.new_df_data.append(
                    [
                        df[const.POLKADOT_UTC_TIME_COLUMN][i],
                        const.EARN_OPERATION,
                        const.POLKADOT_EARN_DESCRIPTION,
                        json.dumps(
                            {
                                "amount": float(
                                    self.__get_earn(
                                        df[const.POLKADOT_EXTRINSIC_ID_COLUMN][i]
                                    ).copy_abs()
                                ),
                                "coin": "DOT",
                            }
                        ),
                    ]
                )
            else:
                raise Exception("Unexpected type.", action)

            i += 1

    def __get_fee(self, extrinsic_id):
        self.__check_timeout()
        payload = {"extrinsic_index": extrinsic_id}

        response = requests.post(
            self.extrinsic_url, json=payload, headers=self.headers, timeout=100
        )

        if response.status_code == 200:
            return decimal.Decimal(response.json()["data"]["fee"]) / decimal.Decimal(
                10000000000
            )

        raise Exception("Error response from polkadot API", response)

    def __get_earn(self, extrinsic_id):
        self.__check_timeout()
        payload = {"extrinsic_index": extrinsic_id, "row": 99}

        response = requests.post(
            self.transactions_url, json=payload, headers=self.headers, timeout=100
        )

        if response.status_code == 200:
            for transfer in response.json()["data"]["transfers"]:
                from_account_display = transfer["from_account_display"]
                if "display" in from_account_display and from_account_display[
                    "display"
                ].endswith("(Reward)"):
                    return decimal.Decimal(transfer["amount"])
            raise Exception("Reward amount not found.")

        raise Exception("Error response from polkadot API.", response)

    def __check_timeout(self):
        self.call_counter = self.call_counter + 1
        if self.call_counter > 3:
            time.sleep(1)
            self.call_counter = 0
