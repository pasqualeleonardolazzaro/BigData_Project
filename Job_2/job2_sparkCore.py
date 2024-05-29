#!/usr/bin/env python3
"""Spark application for computing sector and industry-specific stock statistics per year"""

import argparse
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sector and Industry Stock Analysis") \
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
        'sector': fields[3],
        'industry': fields[4],
        'ticker': fields[1],
        'date': fields[10],
        'year': int(fields[10][:4]),
        'open': float(fields[5]),
        'close': float(fields[6]),
        'volume': int(fields[9])
    }


parsed_data = data.map(parse_row)

# Group data by ticker and year
grouped_data = parsed_data.map(lambda x: ((x['industry'], x['ticker'], x['year'], x['sector']), x))


def reduce_func(a, b):
    first_date = min(a['first_date'], b['first_date'])
    last_date = max(a['last_date'], b['last_date'])
    first_close = a['first_close'] if a['first_date'] <= b['first_date'] else b['first_close']
    last_close = b['last_close'] if a['last_date'] <= b['last_date'] else a['last_close']
    total_volume = a['total_volume'] + b['total_volume']
    ticker = a['ticker']
    sector = a['sector']
    return {
        'first_date': first_date,
        'last_date': last_date,
        'first_close': first_close,
        'last_close': last_close,
        'total_volume': total_volume,
        'ticker': ticker,
        'sector': sector
    }


# Ensure each element is a dictionary with required keys before reducing
mapped_data = grouped_data.mapValues(lambda x: {
    'first_date': x['date'],
    'last_date': x['date'],
    'first_close': x['close'],
    'last_close': x['close'],
    'total_volume': x['volume'],
    'ticker': x['ticker'],
    'sector': x['sector']
})

# Aggregate the data
reduced_data = mapped_data.reduceByKey(reduce_func)


# Calculate average volume and percentage change
def calculate_stats(record):
    key, value = record
    first_close = value['first_close']
    last_close = value['last_close']
    percentage_change = ((last_close - first_close) / first_close) * 100 if first_close != 0 else 0
    ticker = value['ticker']
    sector = value['sector']
    return (key[0], key[1], key[2], key[3], {
        'percentage_change': percentage_change,
        'volume': value['total_volume'],
        'ticker': ticker,
        'sector': sector,
        'first_close': first_close,
        'last_close': last_close
    })


stats = reduced_data.map(calculate_stats)
grouped_data_industry = stats.map(lambda x: ((x[0], x[2], x[3]), x[4]))


def reduce_func_2(a, b):
    max_volume = max(a['max_volume'], b['max_volume'])
    if(max(a['max_volume'], b['max_volume']) == a['max_volume']) :
        ticker_maxVolume = a['ticker_maxVolume']
    else:
        ticker_maxVolume = b['ticker_maxVolume']
    max_percentage = max(a['max_percentage'], b['max_percentage'])
    if (max(a['max_percentage'], b['max_percentage']) == a['max_percentage']):
        ticker_maxPercentage = a['ticker_maxPercentage']
    else:
        ticker_maxPercentage = b['ticker_maxPercentage']
    tot_last_close = a['tot_last_close'] + b['tot_last_close']
    tot_first_close = a['tot_first_close'] + b['tot_first_close']
    count = a['count'] + b['count']
    sector = a['sector']
    return {
        'max_volume': max_volume,
        'ticker_maxVolume': ticker_maxVolume,
        'max_percentage': max_percentage,
        'ticker_maxPercentage': ticker_maxPercentage,
        'tot_last_close': tot_last_close,
        'tot_first_close': tot_first_close,
        'count': count,
        'sector': sector
    }

# Ensure each element is a dictionary with required keys before reducing
mapped_data_industry = grouped_data_industry.mapValues(lambda x: {
    'max_volume': x['volume'],
    'ticker_maxVolume': x['ticker'],
    'max_percentage': x['percentage_change'],
    'ticker_maxPercentage': x['ticker'],
    'tot_last_close': x['last_close'],
    'tot_first_close': x['first_close'],
    'count': 1,
    'sector': x['sector']
})


reduced_data_industry = mapped_data_industry.reduceByKey(reduce_func_2)

def calculate_stats2(record):
    key, value = record
    max_volume = value['max_volume']
    ticker_maxVolume = value['ticker_maxVolume']
    max_percentage = value['max_percentage']
    ticker_maxPercentage = value['ticker_maxPercentage']
    tot_last_close = value['tot_last_close']
    tot_first_close = value['tot_first_close']
    count = value['count']
    percentage_change_avg =((tot_last_close-tot_first_close)/tot_first_close)*100
    sector = value['sector']
    return (key[0], key[1], {
        'sector': sector,
        'percentage_change_avg': percentage_change_avg,
        'max_volume': max_volume,
        'ticker_maxVolume': ticker_maxVolume,
        'max_percentage': max_percentage,
        'ticker_maxPercentage': ticker_maxPercentage
    })


fin = reduced_data_industry.map(calculate_stats2)

sorted_result = fin.sortBy(lambda x: (x[2]['sector'], -x[2]['percentage_change_avg']))

# Raccogliere i risultati ordinati
result = sorted_result.collect()

# Stampa i risultati ordinati
for res in result:
    print(res)


output_path = "/home/paleo/BigData/2Proj/Dataset/Job2_spark/output.txt"

with open(output_path, 'w') as file:
    for item in result:
        file.write(f"{item}\n")
