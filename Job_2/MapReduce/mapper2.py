#!/usr/bin/env python3
"""Identity Mapper for chaining reducers in Hadoop"""

import sys

for line in sys.stdin:
        line = line.strip()
        key, value = line.split('\t')
        # Remove outer parentheses and split the key string into its components
        industry, ticker, year, sector = eval(key)
        # Remove outer parentheses from value and retain internal tuples
        first_close, last_close, total_volume = eval(value)

        # Construct the new key and value
        new_key = (industry, year)
        new_value = (first_close, last_close, total_volume, ticker, sector)

        # Output the new key-value pair
        print(f"{new_key}\t{new_value}")
