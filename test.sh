#!/bin/sh
printf  "Testing export functionality"
python -m unittest discover src/tests "*_tests.py"

printf  "\n\nTesting export refactor functionality"
python -m unittest discover src/tests/refactor -p "*_tests.py"