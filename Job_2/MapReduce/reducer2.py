#!/usr/bin/env python3
"""Final Reducer for computing detailed statistics"""

import sys


current_key = None
max_volume = 0
max_percentage_change = -float('inf')
tot_percentage = 0
sector = ""
ticker_max_percentage = ""
ticker_max_volume = ""
tot_first_close = 0
tot_last_close = 0

for line in sys.stdin:
    key, values = line.strip().split('\t')
    first_close, last_close, volume, ticker, sector = eval(values)
    percentage_change = (((last_close - first_close) / first_close) * 100) if first_close else 0
    tot_first_close = tot_first_close + first_close
    tot_last_close = tot_last_close + last_close

    if key != current_key:
        if current_key:
            percentage_change_industry= ((tot_last_close - tot_first_close) / tot_first_close) * 100 if tot_first_close != 0 else 0
            print(f"{current_key}\t{sector}\t{percentage_change_industry}\t{ticker_max_volume}\t{max_volume}\t{ticker_max_percentage}\t{max_percentage_change}")

        current_key = key
        max_volume = volume
        max_percentage_change = percentage_change
        tot_percentage = percentage_change
        ticker_max_volume = ticker
        ticker_max_percentage = ticker
    else:
        if volume > max_volume:
            max_volume = volume
            ticker_max_volume = ticker
        if percentage_change > max_percentage_change:
            max_percentage_change = percentage_change
            ticker_max_percentage = ticker
        tot_percentage += percentage_change

if current_key:
    percentage_change_industry= ((tot_last_close - tot_first_close) / tot_first_close) * 100 if tot_first_close != 0 else 0
    print(f"{current_key}\t{sector}\t{percentage_change_industry}\t{ticker_max_volume}\t{max_volume}\t{ticker_max_percentage}\t{max_percentage_change}")

