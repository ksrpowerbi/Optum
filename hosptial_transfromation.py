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

hos_df = read_bronze_csv("Hospital")

# COMMAND ----------

display(hos_df.limit(20))

# COMMAND ----------

df_row_columns(hos_df)

# COMMAND ----------

check_missing_perc_drop(hos_df)

# COMMAND ----------

check_string_as_NaN(hos_df)

# COMMAND ----------

hos_df = hos_df.replace("NaN",None)

# COMMAND ----------

display(hos_df.limit(20))

# COMMAND ----------

missing_values_cnt(hos_df)

# COMMAND ----------

#actual transformation
hos_df = hos_df.fillna({'state':'UT'}) 
hos_df = hos_df.replace("New Delhi","Delhi")

# COMMAND ----------

display(hos_df)

# COMMAND ----------

write2silver(hos_df, "Hospital_S.csv")