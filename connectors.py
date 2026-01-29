# Databricks notebook source
def adls_connector():
    spark.conf.set("fs.azure.account.key.optumpadls.dfs.core.windows.net",dbutils.secrets.get(scope="optumscope", key="optumadlskey"))
    return "Connected to bronze"

# COMMAND ----------

from pyspark.sql import DataFrameWriter

# COMMAND ----------

def libraries():
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrameWriter
    return "required libaries imported successfully"


# COMMAND ----------

def list_bronze_files():
    display(dbutils.fs.ls("abfss://optum@optumpadls.dfs.core.windows.net/medallion/bronze"))
    return "Files Listed"

# COMMAND ----------

def list_silver_files():
    display(dbutils.fs.ls("abfss://optum@optumpadls.dfs.core.windows.net/medallion/silver"))
    return "Files Listed"

# COMMAND ----------

def read_bronze_csv(df):
    data = spark.read.csv("abfss://optum@optumpadls.dfs.core.windows.net/medallion/bronze/" + df +".csv", inferSchema=True, header=True)
    print("Data Read Successfully")
    return data

# COMMAND ----------

def read_bronze_json(df):
    data = spark.read.json("abfss://optum@optumpadls.dfs.core.windows.net/medallion/bronze/" + df +".json")
    print("Data Read Successfully")
    return data

# COMMAND ----------

def read_silver_csv(df):
    data = spark.read.csv("abfss://optum@optumpadls.dfs.core.windows.net/medallion/silver/" + df +".csv", inferSchema=True, header=True)
    print("Data Read Successfully")
    return data

# COMMAND ----------

libraries()

# COMMAND ----------

def write2silver(df, file_name):
    silver_path = "abfss://optum@optumpadls.dfs.core.windows.net/medallion/silver"
    temp_path = f"{silver_path}/output_temp"
    final_path = f"{silver_path}/{file_name}"
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [f.path for f in files if f.name.endswith(".csv")][0]
    dbutils.fs.mv(csv_file,final_path)
    dbutils.fs.rm(temp_path,recurse=True)
    print("File written in silver path")

# COMMAND ----------

def write2gold(df, file_name):
    gold_path = "abfss://optum@optumpadls.dfs.core.windows.net/medallion/gold"
    temp_path = f"{gold_path}/output_temp"
    final_path = f"{gold_path}/{file_name}"
    df.write.mode("overwrite").option("header","true").csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [f.path for f in files if f.name.endswith(".csv")][0]
    dbutils.fs.mv(csv_file,final_path)
    dbutils.fs.rm(temp_path,recurse=True)
    print("File written in gold path")

# COMMAND ----------

def write2sqldatabase(df, table_name):
    server = "optumsev.database.windows.net"
    port = "1433"
    database = "optum_db"
    db_properties= {
    "user":"optum_admin",
    "password":"Welcome@123"}
    serverurl = "jdbc:sqlserver://{0}:{1};database={2}".format(server,port,database)
    output = DataFrameWriter(df)
    output.jdbc(url = serverurl, table = table_name, mode = "overwrite", properties = db_properties)
    print("successfully written in database")

# COMMAND ----------

