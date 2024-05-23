#!/usr/bin/env python3
"""Spark application for computing yearly stock statistics for each company from IPO using RDDs"""

import argparse
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .getOrCreate()

sc = spark.sparkContext

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
args = parser.parse_args()

# Load the dataset
rdd = sc.textFile(args.input_path)
# Extract the header
header = rdd.first()
data = rdd.filter(lambda row: row != header)


# Parse the CSV rows into a structured format
def parse_row(row):
    fields = row.split(',')
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


parsed_data = data.map(parse_row)

# Group data by ticker and year
grouped_data = parsed_data.map(lambda x: ((x['ticker'], x['name'], x['year']), x))


# Define the reduce function
def reduce_func(a, b):
    first_date = min(a['first_date'], b['first_date'])
    last_date = max(a['last_date'], b['last_date'])
    first_close = a['first_close'] if a['first_date'] <= b['first_date'] else b['first_close']
    last_close = b['last_close'] if a['last_date'] <= b['last_date'] else a['last_close']
    min_price = min(a['min_price'], b['min_price'])
    max_price = max(a['max_price'], b['max_price'])
    total_volume = a['total_volume'] + b['total_volume']
    count = a['count'] + b['count']

    return {
        'first_date': first_date,
        'last_date': last_date,
        'first_close': first_close,
        'last_close': last_close,
        'min_price': min_price,
        'max_price': max_price,
        'total_volume': total_volume,
        'count': count
    }


# Ensure each element is a dictionary with required keys before reducing
mapped_data = grouped_data.mapValues(lambda x: {
    'first_date': x['date'],
    'last_date': x['date'],
    'first_close': x['close'],
    'last_close': x['close'],
    'min_price': x['low'],
    'max_price': x['high'],
    'total_volume': x['volume'],
    'count': 1
})

# Aggregate the data
reduced_data = mapped_data.reduceByKey(reduce_func)


# Calculate average volume and percentage change
def calculate_stats(record):
    key, value = record
    first_close = value['first_close']
    last_close = value['last_close']
    percentage_change = ((last_close - first_close) / first_close) * 100 if first_close != 0 else 0
    avg_volume = value['total_volume'] / value['count']
    return (key[0], key[1], key[2], {
        'percentage_change': round(percentage_change, 2),
        'min_price': value['min_price'],
        'max_price': value['max_price'],
        'avg_volume': avg_volume
    })


stats = reduced_data.map(calculate_stats)

# Collect results and group by ticker
results = stats.collect()

# Organize results
final_results = {}
for result in results:
    ticker = result[0]
    name = result[1]
    year = result[2]
    stats = result[3]

    if ticker not in final_results:
        final_results[ticker] = {
            'name': name,
            'yearly_stats': []
        }
    final_results[ticker]['yearly_stats'].append({
        'year': year,
        'percentage_change': stats['percentage_change'],
        'min_price': stats['min_price'],
        'max_price': stats['max_price'],
        'avg_volume': stats['avg_volume']
    })

# Display results
# Write results to a file
output_path = "/home/paleo/BigData/2Proj/Dataset/Job1_spark/output.txt"
with open(output_path, "w") as f:
    for ticker, data in final_results.items():
        f.write(f"Ticker: {ticker}, Name: {data['name']}\n")
        for yearly_stat in data['yearly_stats']:
            f.write(f"  Year: {yearly_stat['year']}, "
                    f"Percentage Change: {yearly_stat['percentage_change']}%, "
                    f"Min Price: {yearly_stat['min_price']}, "
                    f"Max Price: {yearly_stat['max_price']}, "
                    f"Avg Volume: {yearly_stat['avg_volume']}\n")

print(f"Results have been written to {output_path}")
