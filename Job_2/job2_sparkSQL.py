#!/usr/bin/env python3
"""spark application"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, max, first

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

# Inizializza la sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, DateType
# define the custom schema explicitly in the code
custom_schema = StructType([
    StructField(name="Unnamed: 0", dataType=StringType(), nullable=True),
    StructField(name="ticker", dataType=StringType(), nullable=True),
    StructField(name="name", dataType=StringType(), nullable=True),
    StructField(name="sector", dataType=StringType(), nullable=True),
    StructField(name="industry", dataType=StringType(), nullable=True),
    StructField(name="open", dataType=FloatType(), nullable=True),
    StructField(name="close", dataType=FloatType(), nullable=True),
    StructField(name="low", dataType=FloatType(), nullable=True),
    StructField(name="high", dataType=FloatType(), nullable=True),
    StructField(name="volume", dataType=IntegerType(), nullable=True),
    StructField(name="date", dataType=DateType(), nullable=True)])

# Carica il dataset in un DataFrame Spark
df = spark.read.csv(input_filepath, schema=custom_schema).cache()

# Registra il DataFrame come vista SQL
df.createOrReplaceTempView("stock_data")

final_df = spark.sql("""
    WITH PriceChanges AS (
        SELECT
            ticker,
            industry,
            sector,
            year(date) AS year,
            (LAST_VALUE(close) OVER w - FIRST_VALUE(close) OVER w) / FIRST_VALUE(close) OVER w * 100 AS percent_change,
            LAST_VALUE(close) OVER w as last_close,
            FIRST_VALUE(close) OVER w as first_close
        FROM
            stock_data
        WINDOW w AS (PARTITION BY ticker, industry, year(date) ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ),
    IndustryYearlyStats AS (
        SELECT
            industry,
            sector,
            year,
            ((SUM(last_close)-SUM(first_close))/SUM(first_close))*100 AS avg_percent_change,
            MAX(percent_change) AS max_percent_change
        FROM
            PriceChanges
        GROUP BY
            industry, sector, year
    ),
    MaxIncrease AS (
        SELECT
            a.industry,
            a.year,
            a.ticker AS max_increase_ticker,
            a.percent_change AS max_increase_percent
        FROM
            PriceChanges a
        JOIN
            (SELECT industry, year, MAX(percent_change) AS max_percent FROM PriceChanges GROUP BY industry, year) b
        ON
            a.industry = b.industry AND a.year = b.year AND a.percent_change = b.max_percent
    ),
    VolumeData AS (
        SELECT
            ticker,
            industry,
            year(date) AS year,
            SUM(volume) AS total_volume
        FROM
            stock_data
        GROUP BY
            ticker, industry, year(date)
    ),
    MaxVolume AS (
        SELECT
            c.industry,
            c.year,
            c.ticker AS max_volume_ticker,
            c.total_volume
        FROM
            VolumeData c
        JOIN
            (SELECT industry, year, MAX(total_volume) AS max_volume FROM VolumeData GROUP BY industry, year) d
        ON
            c.industry = d.industry AND c.year = d.year AND c.total_volume = d.max_volume
    )
    SELECT DISTINCT
        e.industry,
        e.sector,
        e.year,
        e.avg_percent_change,
        f.max_increase_ticker,
        f.max_increase_percent,
        g.max_volume_ticker,
        g.total_volume
    FROM
        IndustryYearlyStats e
    JOIN
        MaxIncrease f ON e.industry = f.industry AND e.year = f.year
    JOIN
        MaxVolume g ON e.industry = g.industry AND e.year = g.year
    ORDER BY
        e.sector, e.avg_percent_change DESC
""")

final_df.write.mode('overwrite').csv("/home/paleo/BigData/2Proj/Dataset/Job2_spark", header=True)

spark.stop()
