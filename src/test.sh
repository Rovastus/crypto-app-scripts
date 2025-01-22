#!/bin/sh
printf  "Testing export functionality"
python -m unittest discover tests "*_test.py"

printf  "\n\nTesting export refactor functionality"
python -m unittest discover tests/refactor -p "*_test.py"