# Databricks notebook source
df = spark.read.table("db_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@adlsdbetenis.dfs.core.windows.net/regions")

# COMMAND ----------

spark.read.format("delta").load("abfss://silver@adlsdbetenis.dfs.core.windows.net/regions").display()

# COMMAND ----------

