#!/usr/bin/env python3
"""Reducer for sector and industry-specific stock statistics"""

import sys


def reduce_func(values):
    first_close = last_close = None
    first_date = last_date = ""
    max_volume = 0
    ticker_maxVolume = ""
    total_percentage_change = 0
    ticker_maxPercentage = ""
    max_percentage_change = -float('inf')
    count = 0
    sector = ""

    for value in values:
        date, ticker, close, volume, sector = value
        if first_date == "" or date < first_date:
            first_date = date
            first_close = close
        if last_date == "" or date > last_date:
            last_date = date
            last_close = close
        if volume > max_volume:
            max_volume = volume
            ticker_maxVolume = ticker
        percentage_change = ((last_close - first_close) / first_close) * 100 if first_close != 0 else 0
        if percentage_change > max_percentage_change:
            max_percentage_change = percentage_change
            ticker_maxPercentage = ticker
        total_percentage_change += percentage_change
        count += 1

    average_percentage_change = total_percentage_change / count if count > 0 else 0
    return (sector, max_volume, ticker_maxVolume, average_percentage_change, max_percentage_change, ticker_maxPercentage)



current_key = None
current_values = []

for line in sys.stdin:
    key, value_str = line.strip().split('\t')
    value = eval(value_str)

    if current_key and key != current_key:
        results = reduce_func(current_values)
        print(f"{current_key}\t{results}")
        current_values = []

    current_key = key
    current_values.append(value)

# Process the last key
if current_key:
    results = reduce_func(current_values)
    print(f"{current_key}\t{results}")