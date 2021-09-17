#!/bin/bash

base_dir="$(realpath $(dirname "$0")/..)"

cd "$base_dir"
source venvs/eth_etl/bin/activate
sayn run -t group:create_eth_tables
scripts/alert.py
sayn run -t group:extract_eth
scripts/alert.py
deactivate
