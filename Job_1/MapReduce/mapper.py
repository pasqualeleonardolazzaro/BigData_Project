#!/usr/bin/env python3
"""Mapper for processing stock data"""

import sys


def parse_row(row):
    fields = row.strip().split(',')
    try:
        return {
            'ticker': fields[1],
            'name': fields[2],
            'date': fields[10],
            'year': int(fields[10][:4]),
            'open': float(fields[5]),
            'close': float(fields[6]),
            'low': float(fields[7]),
            'high': float(fields[8]),
            'volume': int(fields[9])
        }
    except ValueError:
        return None



for row in sys.stdin:
    stock_data = parse_row(row)
    if stock_data:
        key = (stock_data['ticker'], stock_data['name'], stock_data['year'])
        value = (stock_data['date'], stock_data['close'], stock_data['low'], stock_data['high'], stock_data['volume'])
        print(f"{key}\t{value}")

