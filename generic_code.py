# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def missing_values_cnt(df):
    misssing_data = [(c, df.filter(col(c).isNull()).count()) for c in df.columns]
    df_missing = spark.createDataFrame(misssing_data, ["column_name", "missing_count"])
    display(df_missing)

# COMMAND ----------

def check_string_as_NaN(df):
    result = {}
    for i in df.columns:
        result[i] = df.filter(col(i).like("%NaN%")).count()
    print(result)

# COMMAND ----------

def df_row_columns(df):
    return df.count(), len(df.columns)

# COMMAND ----------

def check_missing_perc_drop(df):
    total_rows = df.count()
    missing_values_less_75 = {}
    lst_cl = df.columns
    for col_name in lst_cl:
        missing_values = df.filter(col(col_name).isNull()).count()
        missing_perc = missing_values / total_rows
        if missing_perc < 0.75:
            missing_values_less_75[col_name] = missing_perc
        else:
            df = df.drop(col_name)
    return display(df.limit(5))

# COMMAND ----------

def check_duplicates(df):
    a = df.select("*").distinct().count()
    b = df.count()
    if a == b:
        print("No duplicates")
    else:
        print("Duplicates found")

# COMMAND ----------

def drop_duplicates(df):
    df = df.dropDuplicates()

# COMMAND ----------

