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

claims_df = read_bronze_json("Claims")

# COMMAND ----------

#display(claims_df.limit(20))

# COMMAND ----------

#transformations specifically for claims
claims_df = claims_df.drop("_id")
claims_df = claims_df.replace("NaN",None)
claims_df = claims_df.fillna({"claim_Or_Rejected": "N"})
claims_df = claims_df.withColumn("claim_amount", claims_df["claim_amount"].cast("integer"))
claims_df = claims_df.withColumn("claim_date", claims_df["claim_date"].cast("date"))

# COMMAND ----------

#display(claims_df)

# COMMAND ----------

#check_string_as_NaN(claims_df)

# COMMAND ----------

write2silver(claims_df, "Claims_S.csv")