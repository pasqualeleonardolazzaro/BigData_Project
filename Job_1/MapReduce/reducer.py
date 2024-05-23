#!/usr/bin/env python3
"""Reducer for computing yearly stock statistics"""

import sys


def reduce_func(values):
    first_date = last_date = first_close = last_close = None
    min_price = float('inf')
    max_price = 0
    total_volume = 0
    count = 0

    for value in values:
        date, close, low, high, volume = value
        if first_date is None or date < first_date:
            first_date = date
            first_close = close
        if last_date is None or date > last_date:
            last_date = date
            last_close = close
        min_price = min(min_price, low)
        max_price = max(max_price, high)
        total_volume += volume
        count += 1

    percentage_change = ((last_close - first_close) / first_close) * 100 if first_close != 0 else 0
    avg_volume = total_volume / count if count != 0 else 0
    return (first_close, last_close, min_price, max_price, total_volume, count, percentage_change, avg_volume)



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

# Handle the last key
if current_key:
    results = reduce_func(current_values)
    print(f"{current_key}\t{results}")
