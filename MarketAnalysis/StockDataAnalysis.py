# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

df = spark.read.format("delta")\
    .load("dbfs:/user/hive/warehouse/tesla_stock_price")

display(df)

# COMMAND ----------

df.select(max("Adj Close"), max("Volume"))\
    .withColumnRenamed("max(Adj Close)", "Max Volume")\
    .show(truncate=False)

# COMMAND ----------


