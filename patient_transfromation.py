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

pat_df = read_bronze_csv("Patient_records")

# COMMAND ----------

display(pat_df.limit(20))

# COMMAND ----------

check_string_as_NaN(pat_df)

# COMMAND ----------

missing_values_cnt(pat_df)

# COMMAND ----------

pat_df = pat_df.fillna({"patient_name":"Vistor/NA"})
pat_df = pat_df.drop('patient_phone')
pat_df = pat_df.withColumn("patient_age", round(months_between(current_date(), col("patient_birth_date"))/12,0).cast("integer"))
pat_df = pat_df.drop('patient_birth_date')

# COMMAND ----------

display(pat_df)

# COMMAND ----------

write2silver(pat_df, "Patient_S.csv")

# COMMAND ----------

#pat_df.select("*").filter(col('Patient_name').rlike("Prakash")).show()

# COMMAND ----------

pat_df.select("*").filter(col('Patient_name').rlike("Paridhi")).show()