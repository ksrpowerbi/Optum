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

grp_df = read_bronze_csv("group")

# COMMAND ----------

display(grp_df.limit(20))

# COMMAND ----------

missing_values_cnt(grp_df)

# COMMAND ----------

check_duplicates(grp_df)

# COMMAND ----------

write2silver(grp_df, "Group_S.csv")