import argparse
import pathlib
import os

cli = argparse.ArgumentParser()
cli.add_argument("-d", "--data", required=True, type=pathlib.Path)
cli.add_argument("-o", "--output", required=True, type=pathlib.Path)
args = cli.parse_args()

print("data: %r" % args.data)
print("output: %r" % args.output)

for root, dirs, files in os.walk(args.data, topdown=False):
    if args.data.name == root:
        continue

    if root.endswith("\binance"):
        print("binance")
        for file in files:
            print(file)
