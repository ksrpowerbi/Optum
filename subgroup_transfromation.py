# Databricks notebook source
# MAGIC %run /Workspace/Shared/optum/connectors

# COMMAND ----------

# MAGIC %run /Workspace/Shared/optum/generic_code

# COMMAND ----------

adls_connector()

# COMMAND ----------

libraries()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

subgrp_df = read_bronze_csv("subgroup")

# COMMAND ----------

display(subgrp_df.limit(20))

# COMMAND ----------

subgrp_df = subgrp_df.withColumn("subgrp_id", split(col("subgrp_id"),","))

# COMMAND ----------

display(subgrp_df)

# COMMAND ----------

subgrp_df = subgrp_df.withColumn("subgrp_id", explode(col("subgrp_id")))

# COMMAND ----------

display(subgrp_df)

# COMMAND ----------

write2silver(subgrp_df, "SubGroup_S.csv")