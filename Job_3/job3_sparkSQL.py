#!/usr/bin/env python3
"""spark application"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, last, first, round
from pyspark.sql.window import Window
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
dfn = spark.read.csv(input_filepath, schema=custom_schema).cache()


df = dfn.withColumn("date", col("date").cast("date"))  # Ensure date is DateType

# Define window specification
year_window = Window.partitionBy("ticker", year("date")).orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Calculate percent changes
annual_changes = df.withColumn("year", year("date")) \
    .filter(col("year") >= 2000) \
    .withColumn("first_close", first("close").over(year_window)) \
    .withColumn("last_close", last("close").over(year_window)) \
    .withColumn("percent_change",
                ((col("last_close").cast("double") - col("first_close").cast("double")) / col("first_close").cast("double")) * 100) \
    .select("ticker", "name", "year", "percent_change")

# Optionally, convert the percent change to integer if needed
annual_changes = annual_changes.withColumn("percent_change", col("percent_change").cast("int"))
annual_changes.createOrReplaceTempView("AnnualPercentChange")

# Define a query to find three-year trends
three_year_trends_query = """
SELECT DISTINCT
    a.ticker,
    a.name,
    CONCAT_WS(',', CAST(a.year AS STRING), CAST((a.year + 1) AS STRING), CAST((a.year + 2) AS STRING)) AS year_sequence,
    CONCAT_WS(':', CAST(a.year AS STRING), CAST(ROUND(a.percent_change, 2) AS STRING)) AS first_year_trend,
    CONCAT_WS(':', CAST((a.year + 1) AS STRING), CAST(ROUND(b.percent_change, 2) AS STRING)) AS second_year_trend,
    CONCAT_WS(':', CAST((a.year + 2) AS STRING), CAST(ROUND(c.percent_change, 2) AS STRING)) AS third_year_trend
FROM
    AnnualPercentChange a
JOIN
    AnnualPercentChange b ON a.ticker = b.ticker AND b.year = a.year + 1
JOIN
    AnnualPercentChange c ON a.ticker = c.ticker AND c.year = a.year + 2
"""
three_year_trends = spark.sql(three_year_trends_query)
three_year_trends.createOrReplaceTempView("ThreeYearTrends")

# Define the final aggregation query
final_result_query = """
SELECT
    COLLECT_SET(name) AS companies,
    CONCAT_WS(', ', first_year_trend, second_year_trend, third_year_trend) AS trends
FROM
    ThreeYearTrends
GROUP BY
    trends
HAVING
    SIZE(companies) > 1
"""
final_result = spark.sql(final_result_query)
# Show results
final_result.show(truncate=False)

from pyspark.sql.functions import array_join

# Assuming 'companies' is the column with the array of strings
final_result = final_result.withColumn("companies", array_join(col("companies"), ", "))

final_result.write.mode('overwrite').csv("/home/paleo/BigData/2Proj/Dataset/Job3_spark20", header=True)

spark.stop()
