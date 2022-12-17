import argparse
import pathlib
import os
import pandas as pd

import constant as const
import refactor.binance_export_refactor as ber
import refactor.kraken_export_refactor as ker
import binance_export as be
import kraken_export as ke


def __refactor_binance_export(root, files, year, output):
    transactions = pd.read_csv(root + "\\" + "transactions.csv")
    export_refactor = ber.BinanceExportRefactor()
    ref_files = []

    for file in files:
        if file.startswith(year):
            print("Refactoring binance export file: " + root + "\\" + file)
            export = pd.read_csv(root + "\\" + file)
            export_refactor.refactor(export, transactions)
            ref_file_name = output + "\\ref_binance_" + file
            print("Saving refactored file: " + ref_file_name)
            export.to_csv(ref_file_name, index=False)
            ref_files.append(ref_file_name)

    return ref_files


def __crypto_app_binance_export(root, ref_files, output):
    crypto_app_export = None

    for file in ref_files:
        print("Processing binance export: " + root + "\\" + file)
        binance_export = be.BinanceExport()
        export = pd.read_csv(root + "\\" + file)
        binance_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = binance_export.get_df()
        else:
            crypto_app_export = crypto_app_export.append(binance_export.get_df())

    print("Saving crypto-app binance export file: " + output + "\\" + "binance.csv")
    crypto_app_export.to_csv(output + "\\" + "binance.csv", index=False)


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
            export.to_csv(ref_file_name, index=False)
            ref_files.append(ref_file_name)

    return ref_files


def __crypto_app_kraken_export(root, ref_files, output):
    crypto_app_export = None

    for file in ref_files:
        print("Processing kraken export: " + root + "\\" + file)
        kraken_export = ke.KrakenExport()
        export = pd.read_csv(root + "\\" + file)
        kraken_export.read_export(export)
        if crypto_app_export is None:
            crypto_app_export = kraken_export.get_df()
        else:
            crypto_app_export = crypto_app_export.append(kraken_export.get_df())

    print("Saving crypto-app kraken export file: " + output + "\\" + "kraken.csv")
    crypto_app_export.to_csv(output + "\\" + "kraken.csv", index=False)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("-d", "--data", required=True, type=pathlib.Path)
    cli.add_argument("-o", "--output", required=True, type=pathlib.Path)
    cli.add_argument("-y", "--year", required=True, type=str)
    args = cli.parse_args()

    print("data: %r" % args.data)
    print("output: %r" % args.output)
    if os.listdir(args.output) != []:
        raise Exception("Output folder not empty.")
    print("year: %s" % args.year)
    print("---------------------")

    for root, dirs, files in os.walk(args.data, topdown=False):
        if args.data.name == root:
            continue

        if root.endswith(const.BINANCE_DIR):
            print("Binance")
            ref_files = __refactor_binance_export(
                root, files, args.year, str(args.output)
            )
            __crypto_app_binance_export(root, ref_files, str(args.output))

        if root.endswith(const.KRAKEN_DIR):
            print("Kraken")
            ref_files = __refactor_kraken_export(
                root, files, args.year, str(args.output)
            )
            __crypto_app_kraken_export(root, ref_files, str(args.output))

        if root.endswith(const.BTC_DIR):
            print("BTC")
            for file in files:
                print(file)

        if root.endswith(const.ETH_DIR):
            print("ETH")
            for file in files:
                print(file)

        if root.endswith(const.DOT_DIR):
            print("DOT")
            for file in files:
                print(file)

        if root.endswith(const.SOL_DIR):
            print("SOL")
            for file in files:
                print(file)
