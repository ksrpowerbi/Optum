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

subsci_df = read_bronze_csv("subscriber")

# COMMAND ----------

#display(subsci_df.limit(20))

# COMMAND ----------

#check_string_as_NaN(subsci_df)

# COMMAND ----------

#missing_values_cnt(subsci_df)

# COMMAND ----------

subsci_df = subsci_df.fillna({"first_name":"Vistor/NA", "Elig_ind":"N"})
subsci_df = subsci_df.drop('Phone')
subsci_df = subsci_df.withColumn("suscriber_age", round(months_between(current_date(), col("Birth_date"))/12,0).cast("integer"))
subsci_df = subsci_df.drop('Birth_date')


# COMMAND ----------

#missing_values_cnt(subsci_df)

# COMMAND ----------

#subsci_df.select("*").filter(col('Subgrp_id').isNull()).show(5)      #Prakash --> 134184 | Flu|  i need to fill S110
                                                                     #Paridhi --> 121783 | Bladder cancer| i need to fill S107

# COMMAND ----------

subsci_df = subsci_df.withColumn("Subgrp_id",when((col("Subgrp_id").isNull()) & (col("sub_id")=='SUBID10022'),'S110') \
                                           .when((col("Subgrp_id").isNull()) & (col("sub_id")=='SUBID10049'),'S107') \
                                           .otherwise(col("Subgrp_id")))

# COMMAND ----------

#display(subsci_df.limit(20))

# COMMAND ----------

#subsci_df.select("*").filter(col('sub_id').isin('SUBID10022','SUBID10049')).show(5)


# COMMAND ----------

write2silver(subsci_df, "Subscriber_S.csv")