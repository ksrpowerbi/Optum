# Databricks notebook source
# MAGIC %run /Workspace/Shared/optum/connectors

# COMMAND ----------

# MAGIC %run /Workspace/Shared/optum/generic_code

# COMMAND ----------

adls_connector()

# COMMAND ----------

libraries()

# COMMAND ----------

#list_bronze_files()

# COMMAND ----------

dis_df = read_bronze_csv("disease")

# COMMAND ----------

#display(dis_df.limit(20))

# COMMAND ----------


#missing_values_cnt(dis_df)

# COMMAND ----------

#check_duplicates(dis_df)

# COMMAND ----------

#check_string_as_NaN(dis_df)

# COMMAND ----------

write2silver(dis_df, "Disease_S.csv")

# COMMAND ----------

dis_df.select("*").filter(col('disease_name').like("%Flu%")).show(5)    #110059 | S110


# COMMAND ----------

dis_df.select("*").filter(col('disease_name').like("%Bladder cancer%")).show(5)    #110039 | S107