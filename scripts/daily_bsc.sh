#!/bin/bash

sayn run -t group:create_bsc_tables -t group:extract_bsc -t stg_dates -t group:xwg -t group:xwg_data_dump
scripts/alert.py
