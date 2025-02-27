import constant as const


class KrakenExportRefactor:
    def __init__(self):
        self.asset = {
            "1INCH": "1INCH",
            "AAVE": "AAVE",
            "ACA": "ACA",
            "ACH": "ACH",
            "ADA": "ADA",
            "ADX": "ADX",
            "AGLD": "AGLD",
            "AIR": "AIR",
            "AKT": "AKT",
            "ALCX": "ALCX",
            "ALGO": "ALGO",
            "ALICE": "ALICE",
            "ANKR": "ANKR",
            "ANT": "ANT",
            "APE": "APE",
            "API3": "API3",
            "APT": "APT",
            "ARPA": "ARPA",
            "ASTR": "ASTR",
            "ATLAS": "ATLAS",
            "ATOM": "ATOM",
            "AUDIO": "AUDIO",
            "AVAX": "AVAX",
            "BTC": "BTC",
            "DOT": "DOT",
            "ETH": "ETH",
            "ETH2": "ETH2",
            "ETHW": "ETHW",
            "GLMR": "GLMR",
            "KILT": "KILT",
            "KSM": "KSM",
            "LINK": "LINK",
            "MOVR": "MOVR",
            "SOL": "SOL",
            "STRK": "STRK",
            "UNI": "UNI",
            "XETH": "ETH",
            "XLTC": "LTC",
            "XBT": "BTC",
            "XXBT": "BTC",
            "ZEUR": "EUR",
            "EUR": "EUR",
            "ZUSD": "USD",
        }

    def refactor(self, df):
        # refactor asset column and change fee to negative value
        for i in df.index:
            df.loc[i, const.KRAKEN_ASSET_COLUMN] = self.__get_converted_asset(
                df[const.KRAKEN_ASSET_COLUMN][i]
            )

    def __get_converted_asset(self, asset):
        if asset.count(".") == 1:
            asset = asset.split(".")[0]

        if asset in self.asset:
            return self.asset[asset]

        raise Exception("Provided asset is not in list.", asset)
