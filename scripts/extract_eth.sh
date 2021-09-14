#!/bin/bash

base_dir="$(realpath $(dirname "$0")/..)"

cd "$base_dir"
source venv/bin/activate
sayn run -t create_tables -t extract_eth
scripts/alert.py
deactivate
