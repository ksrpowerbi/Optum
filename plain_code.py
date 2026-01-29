# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list(scope="optumscope"))

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.optumpadls.dfs.core.windows.net",
    dbutils.secrets.get(scope="optumscope", key="optumadlskey"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://optum@optumpadls.dfs.core.windows.net/medallion/bronze"))

# COMMAND ----------

hos_df = spark.read.csv('abfss://optum@optumpadls.dfs.core.windows.net/medallion/bronze/Hospital.csv', inferSchema=True, header=True)

# COMMAND ----------

display(hos_df)

# COMMAND ----------

for i in hos_df.columns:
    values = {i:hos_df.filter(col(i).isNull()).count()}
    print(values)

# COMMAND ----------

