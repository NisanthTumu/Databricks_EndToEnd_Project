# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/orders")

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df.limit(5).display()

# COMMAND ----------

df = df.withColumn("year", year(col("order_date")))
df.limit(5).display()

# COMMAND ----------

df1 = df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

df1 = df1.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

df1 = df1.withColumn("row_flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes - OOP**

# COMMAND ----------

class window:

  def dense_rank(self,df):

    df_dense_rank = df.withColumn("flag",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_dense_rank
  
  def rank(self,df):

    df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_rank
  
  def row_number(self,df):

    df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

    return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

obj = window()

df_res = obj.dense_rank(df_new)
df_res.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@adlsdbetenis.dfs.core.windows.net/orders")

# COMMAND ----------

spark.read.format("delta").load("abfss://silver@adlsdbetenis.dfs.core.windows.net/orders").display()

# COMMAND ----------

