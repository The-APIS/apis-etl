#!/bin/bash

base_dir="$(realpath $(dirname "$0")/..)"

cd "$base_dir"
source venvs/bsc_etl/bin/activate
sayn run -t group:create_bsc_tables
scripts/alert.py
sayn run -t group:extract_bsc
scripts/alert.py
deactivate
