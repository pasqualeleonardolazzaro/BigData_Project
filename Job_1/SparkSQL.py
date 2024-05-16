#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, round, avg, first, last, min, max, collect_list, struct

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

# Estrai l'anno dalla colonna 'date'
df = df.withColumn("year", year(df["date"]))

df.show(truncate=False)

# Calcola la variazione percentuale della quotazione nell'anno
# df_result0 = df.groupBy("ticker", "year").agg(
#     ((last("close") - first("close")) / first("close") * 100).alias("price_change_percent")
# )

# Calcola il prezzo minimo e massimo nell'anno
# Stampa lo schema del DataFrame per verificare la presenza della colonna "low"
print("Schema:")
df.printSchema()

# # Seleziona le colonne necessarie prima di eseguire l'operazione di aggregazione
# df_selected = df.select("ticker", "name", "year", "low", "high", "volume")
df = df.orderBy("ticker", "name", "date")
# Calcola il prezzo minimo e massimo nell'anno
df_result = df.groupBy("ticker", "name", "year").agg(
    round(((last("close") - first("close")) / first("close") * 100), 2).alias("price_change_percent"),
    round(min("low"), 2).alias("min_price"),
    round(max("high"), 2).alias("max_price"),
    round(avg("volume"), 2).alias("avg_volume")
)

df_result.show(truncate=False)

df_result_ordered = df_result.orderBy("ticker", "year")

# Mostra il risultato ordinato
df_result_ordered.show(truncate=False)

# Raggruppa i dati per ticker e nome dell'azienda e crea una lista di tutte le statistiche per ogni anno
df_list = df_result.groupBy("ticker", "name").agg(
    collect_list(
        struct(
            "year",
            struct(
                "price_change_percent",
                "min_price",
                "max_price",
                "avg_volume"
            ).alias("stats")
        )
    ).alias("yearly_stats")
)

# Mostra il risultato
df_list.show(truncate=False)
df_list.write.csv("/home/paleo/BigData/2Proj/Dataset/Job1_spark/job1_spark.csv")
# Termina la sessione Spark
spark.stop()
