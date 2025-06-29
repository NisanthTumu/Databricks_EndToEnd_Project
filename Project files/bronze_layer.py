# Databricks notebook source
# MAGIC %md
# MAGIC ## Dynamic Capabalities

# COMMAND ----------

dbutils.widgets.text("file_name","")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://source@adlsdbetenis.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation",f"abfss://bronze@adlsdbetenis.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://source@adlsdbetenis.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointlocation",f"abfss://bronze@adlsdbetenis.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path",f"abfss://bronze@adlsdbetenis.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

df_ord = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/orders")
df_ord.display()


# COMMAND ----------

df_cus = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/customers")
df_cus.display()

# COMMAND ----------

df_prod = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/products")
df_prod.display()