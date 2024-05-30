#!/usr/bin/env python3
"""Mapper for extracting stock data by industry and sector"""

import sys


def parse_row(row):
    fields = row.strip().split(',')
    try:
        # Ensure all fields exist to avoid IndexError
        if len(fields) < 11:
            raise ValueError("Not enough columns")
        return (
            (fields[3], fields[4], fields[10][:4]),  # Key: Sector, Industry, Year
            (fields[1], float(fields[6]), int(fields[9]), fields[10])  # Values: Ticker, Close, Volume, Date
        )
    except (IndexError, ValueError) as e:
        sys.stderr.write(f"Error parsing row: {row}, Error: {e}\n")
        return None


for line in sys.stdin:
    parsed = parse_row(line)
    if parsed:
        print(f"{parsed[0]}\t{parsed[1]}")

