#!/bin/bash

base_dir="$(realpath $(dirname "$0")/..)"

cd "$base_dir"
source venvs/bsc_etl/bin/activate
sayn run -p prod -t group:create_bsc_tables
scripts/alert.py
sayn run -p prod -t group:extract_bsc
scripts/alert.py
deactivate
