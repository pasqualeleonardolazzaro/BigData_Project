#!/usr/bin/env python3
"""Reducer for aggregating stock data by industry and sector"""

import sys


def reduce_func(values):
    first_date = last_date = first_close = last_close = None
    total_volume = 0

    for date, close, volume in values:
        if first_date is None or date < first_date:
            first_date = date
            first_close = close
        if last_date is None or date > last_date:
            last_date = date
            last_close = close
        total_volume += volume

    return (first_close, last_close, total_volume)



current_key = None
current_values = []

for line in sys.stdin:
    key, value_str = line.strip().split('\t')
    value = eval(value_str)

    if current_key and key != current_key:
        result = reduce_func(current_values)
        print(f"{current_key}\t{result}")
        current_values = []

    current_key = key
    current_values.append(value)

if current_key:
    result = reduce_func(current_values)
    print(f"{current_key}\t{result}")