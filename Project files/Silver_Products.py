# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@adlsdbetenis.dfs.core.windows.net/products")
df.limit(5).display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function db_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.95

# COMMAND ----------

# MAGIC %sql
# MAGIC  select product_id, price, db_cata.bronze.discount_func(price) as discounted_price
# MAGIC  from products
# MAGIC  limit 10

# COMMAND ----------

df = df.withColumn("discounted_price", expr("db_cata.bronze.discount_func(price)"))

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function db_cata.bronze.upper_func(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as 
# MAGIC $$ return p_brand.upper() $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, db_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products
# MAGIC limit 10

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@adlsdbetenis.dfs.core.windows.net/products")

# COMMAND ----------

spark.read.format("delta").load("abfss://silver@adlsdbetenis.dfs.core.windows.net/products").limit(5).display()

# COMMAND ----------

