import argparse
import os
import pathlib

import pandas as pd

import binance_earning_export as bee
import binance_transaction_export as bte
import binance_withdrawal_export as bwe
import constant as const
import kraken_export as ke
import polkadot_export as dot
import refactor.kraken_export_refactor as ker
import solana_export as sol


def __crypto_app_binance_export(root, output, transactions, earnings, withdrawals):
    crypto_app_export = None

    for transaction in transactions:
        print("Processing binance transaction export: " + root + "\\" + transaction)
        binance_transaction_export = bte.BinanceTransactionExport()
        export = pd.read_csv(root + "\\" + transaction)
        binance_transaction_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = binance_transaction_export.get_df()
        else:
            crypto_app_export = pd.concat(
                [crypto_app_export, binance_transaction_export.get_df()]
            )

    for earning in earnings:
        print("Processing binance earning export: " + root + "\\" + earning)
        binance_earning_export = bee.BinanceEarningExport()
        export = pd.read_csv(root + "\\" + earning)
        binance_earning_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = binance_earning_export.get_df()
        else:
            crypto_app_export = pd.concat(
                [crypto_app_export, binance_earning_export.get_df()]
            )

    for withdrawal in withdrawals:
        print("Processing binance withdrawal export: " + root + "\\" + withdrawal)
        binance_withdrawal_export = bwe.BinanceWithdrawalExport()
        export = pd.read_csv(root + "\\" + withdrawal)
        binance_withdrawal_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = binance_withdrawal_export.get_df()
        else:
            crypto_app_export = pd.concat(
                [crypto_app_export, binance_withdrawal_export.get_df()]
            )

    print("Saving crypto-app binance export file: " + output + "\\binance.csv")
    crypto_app_export.to_csv(
        output + "\\binance.csv", index=False, sep=";", quotechar="'"
    )


def __refactor_kraken_export(root, files, year, output):
    export_refactor = ker.KrakenExportRefactor()
    ref_files = []

    for file in files:
        if file.startswith(year):
            print("Refactoring kraken export file: " + root + "\\" + file)
            export = pd.read_csv(root + "\\" + file)
            export_refactor.refactor(export)
            ref_file_name = output + "\\ref_kraken_" + file
            print("Saving refactored file: " + ref_file_name)
            export.to_csv(ref_file_name, index=False, sep=";", quotechar="'")
            ref_files.append(ref_file_name)

    return ref_files


def __crypto_app_kraken_export(root, ref_files, output):
    crypto_app_export = None

    for file in ref_files:
        print("Processing kraken export: " + root + "\\" + file)
        kraken_export = ke.KrakenExport()
        export = pd.read_csv(root + "\\" + file, sep=";", quotechar="'")
        kraken_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = kraken_export.get_df()
        else:
            crypto_app_export = pd.concat([crypto_app_export, kraken_export.get_df()])

    print("Saving crypto-app kraken export file: " + output + "\\" + "kraken.csv")
    crypto_app_export.to_csv(
        output + "\\" + "kraken.csv", index=False, sep=";", quotechar="'"
    )


def __crypto_app_solana_export(root, ref_files, output):
    crypto_app_export = None

    for file in ref_files:
        print("Processing solana export: " + root + "\\" + file)
        solana_export = sol.SolanaExport()
        export = pd.read_csv(root + "\\" + file)
        solana_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = solana_export.get_df()
        else:
            crypto_app_export = pd.concat([crypto_app_export, solana_export.get_df()])

    print("Saving crypto-app solana export file: " + output + "\\" + "solana.csv")
    crypto_app_export.to_csv(
        output + "\\" + "solana.csv", index=False, sep=";", quotechar="'"
    )


def __crypto_app_polkadot_export(root, ref_files, output):
    crypto_app_export = None

    for file in ref_files:
        print("Processing polkadot export: " + root + "\\" + file)
        polkadot_export = dot.PolkadotExport()
        export = pd.read_csv(root + "\\" + file)
        polkadot_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = polkadot_export.get_df()
        else:
            crypto_app_export = pd.concat([crypto_app_export, polkadot_export.get_df()])

    print("Saving crypto-app polkadot export file: " + output + "\\" + "polkadot.csv")
    crypto_app_export.to_csv(
        output + "\\" + "polkadot.csv", index=False, sep=";", quotechar="'"
    )


def main():
    cli = argparse.ArgumentParser()
    cli.add_argument("-d", "--data", required=True, type=pathlib.Path)
    cli.add_argument("-o", "--output", required=True, type=pathlib.Path)
    cli.add_argument("-y", "--year", required=True, type=str)
    args = cli.parse_args()

    print(f"data: {args.data}")
    print(f"output: {args.output}")
    if os.listdir(args.output) != []:
        raise Exception("Output folder not empty.")
    print(f"year: {args.year}")
    print("---------------------")

    for root, _, files in os.walk(args.data, topdown=False):
        if args.data.name == root:
            continue

        if root.endswith(const.BINANCE_DIR_PATH):
            print("Binance")
            withdrawals = [
                w for w in files if w.startswith(const.BINANCE_WITHDRAWAL_EXPORT_NAME)
            ]
            transactions = [
                t for t in files if t.startswith(const.BINANCE_TRANSACTION_EXPORT_NAME)
            ]
            earnings = [
                e for e in files if e.startswith(const.BINANCE_EARNING_EXPORT_NAME)
            ]

            __crypto_app_binance_export(
                root, str(args.output), transactions, earnings, withdrawals
            )

        if root.endswith(const.KRAKEN_DIR_PATH):
            print("Kraken")
            ref_files = __refactor_kraken_export(
                root, files, args.year, str(args.output)
            )
            __crypto_app_kraken_export(root, ref_files, str(args.output))

        if root.endswith(const.BTC_DIR_PATH):
            print("BTC")
            for file in files:
                print(file)

        if root.endswith(const.ETH_DIR_PATH):
            print("ETH")
            for file in files:
                print(file)

        if root.endswith(const.DOT_DIR_PATH):
            print("DOT")
            __crypto_app_polkadot_export(root, files, str(args.output))

        if root.endswith(const.SOL_DIR_PATH):
            print("SOL")
            __crypto_app_solana_export(root, files, str(args.output))

    # create final file
    final_files = []
    for root, _, files in os.walk(args.output, topdown=False):
        for file in files:
            if not file.startswith("ref_"):
                final_files.append(str(args.output) + "\\" + file)

    df_list = [pd.read_csv(file, sep=";", quotechar="'") for file in final_files]
    final_df = pd.concat(df_list)
    final_df = final_df.sort_values(by=const.UTC_TIME_COLUMN)
    print("Saving final export file: " + str(args.output) + "\\final.csv")
    final_df.to_csv(
        str(args.output) + "\\final.csv", index=False, sep=";", quotechar="'"
    )


if __name__ == "__main__":
    main()
