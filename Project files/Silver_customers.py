# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/customers")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.limit(5).display()

# COMMAND ----------

df = df.withColumn("domains", split(col("email"), "@")[1])
df.limit(5).display()

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("customers_count")).sort(desc("customers_count")).display()

# COMMAND ----------

df_gmail = df.filter(col("domains") == "gmail.com")
df_gmail.limit(5).display()
time.sleep(5)

df_yahoo = df.filter(col("domains")== "yahoo.com")
df_yahoo.limit(5).display()
time.sleep(5)

df_hotmail = df.filter(col("domains")=="hotmail.com")
df_hotmail.limit(5).display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn("full_name", concat(col("first_name"),lit(' '),col("last_name")))
df= df.drop("first_name","last_name")
df.limit(5).display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@adlsdbetenis.dfs.core.windows.net/customers")

# COMMAND ----------

spark.read.format("delta").load("abfss://silver@adlsdbetenis.dfs.core.windows.net/customers").limit(5).display()

# COMMAND ----------

