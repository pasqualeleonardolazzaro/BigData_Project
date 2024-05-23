#!/usr/bin/env python3
"""Mapper for processing sector and industry-specific stock data"""

import sys

def parse_row(row):
    fields = row.strip().split(',')
    try:
        return {
            'sector': fields[3],
            'industry': fields[4],
            'ticker': fields[1],
            'date': fields[10],
            'year': fields[10][:4],
            'close': float(fields[6]),
            'volume': int(fields[9])
        }
    except ValueError:
        return None


for row in sys.stdin:
    stock_data = parse_row(row)
    if stock_data:
        key = (stock_data['industry'],  stock_data['year'])
        value = (stock_data['date'], stock_data['ticker'], stock_data['close'], stock_data['volume'], stock_data['sector'])
        print(f"{key}\t{value}")

