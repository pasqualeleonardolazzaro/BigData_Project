#!/usr/bin/env python3
import sys
from collections import defaultdict


def parse_row(row):
    fields = row.strip().split(',')
    try:
        return {
            'ticker': fields[1],
            'date': fields[10],
            'close': float(fields[6]),
            'name': fields[2],
            'year': int(fields[10][:4])
        }
    except ValueError:
        return None


data = defaultdict(lambda: {'close_prices': [], 'name': ''})

header = True
# Lettura dei dati e organizzazione
for row in sys.stdin:
    if header:
        header = False
        continue
    stock_data = parse_row(row)
    if stock_data['year'] >= 2000:
        data[(stock_data['year'], stock_data['ticker'])]['close_prices'].append((stock_data['date'], stock_data['close']))
        data[(stock_data['year'], stock_data['ticker'])]['name'] = stock_data['name']


# Ordinamento e calcolo della variazione percentuale
for key in data:
    # Ordina i prezzi di chiusura in base alla data
    data[key]['close_prices'].sort(key=lambda x: x[0])
    close_prices = [price for date, price in data[key]['close_prices']]

    if len(close_prices) > 2:
        start_price = close_prices[0]
        end_price = close_prices[-1]
        percentage_change = ((end_price - start_price) / start_price) * 100
        print(f"{key[1]}\t{key[0]}\t{percentage_change:.1f}\t{data[key]['name']}")

