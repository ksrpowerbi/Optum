# Databricks notebook source
# MAGIC %run /Workspace/Shared/optum/connectors

# COMMAND ----------

from pyspark.sql import DataFrameWriter

# COMMAND ----------

adls_connector()

# COMMAND ----------

list_silver_files()

# COMMAND ----------

Claims_df = read_silver_csv("Claims_S")
Disease_df = read_silver_csv("Disease_S")
Group_df = read_silver_csv("Group_S")
hos_df = read_silver_csv("Hospital_S")
pat_df = read_silver_csv("Patient_S")
SubGroup_df = read_silver_csv("SubGroup_S")
Subscriber_df = read_silver_csv("Subscriber_S")

# COMMAND ----------

display(pat_df.limit(5))

# COMMAND ----------

display(hos_df.limit(5))

# COMMAND ----------

pat_hos_df = pat_df.join(hos_df, pat_df.hospital_id == hos_df.Hospital_id, how='inner')
display(pat_hos_df.limit(5))

# COMMAND ----------

display(Group_df.limit(5))

# COMMAND ----------

display(SubGroup_df.limit(5))

# COMMAND ----------

grp_subgrp_df = Group_df.join(SubGroup_df, Group_df.grp_id == SubGroup_df.subgrp_id, how='inner')

# COMMAND ----------

display(grp_subgrp_df.limit(5))

# COMMAND ----------

display(Claims_df)

# COMMAND ----------

pat_hos_claims_df = pat_hos_df.join(Claims_df, pat_hos_df.Patient_id == Claims_df.patient_id, how='inner')

# COMMAND ----------

display(pat_hos_claims_df.limit(5))

# COMMAND ----------

display(Subscriber_df.limit(5))

# COMMAND ----------

optum_df = pat_hos_claims_df.join(Subscriber_df, Subscriber_df.sub_id == pat_hos_claims_df.SUB_ID, how='inner')

# COMMAND ----------

optum_df.columns

# COMMAND ----------

optum_df1 = optum_df.select(
 'Patient_name',
 'patient_gender',
 'patient_age',
 'Hospital_name')

# COMMAND ----------

display(optum_df1)

# COMMAND ----------

write2gold(optum_df,'Optum_G.csv')
write2sqldatabase(optum_df1, 'Optum_Tb')

# COMMAND ----------

merged_df = Patient_df \
    .join(Hospital_df, Patient_df['hospital_id'] == Hospital_df['hospital_id']) \
    .join(Claims_df, Patient_df['Patient_id'] == Claims_df['patient_id']) \
    .join(Subscriber_S_df, Subscriber_S_df['sub_id'] == Claims_df['SUB_ID']) \
    .drop(Hospital_df["hospital_id"]) \
    .drop(Claims_df["patient_id"]) \
    .drop(Subscriber_S_df["sub_id"]) \
    .drop(Patient_df["city"])\
    .drop(Disease_df['disease_name']) \
    .drop(Claims_df['disease_name']) \
    .drop(Hospital_df['country']) \
    .drop(Hospital_df['City']) \
    .drop(Patient_df['patient_gender']) \
    .drop(Patient_df['patient_age'])