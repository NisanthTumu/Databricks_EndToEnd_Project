# Databricks notebook source
# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Table**

# COMMAND ----------

# Expectations

my_rules ={
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL",
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream \
        .format("delta") \
        .option("skipChangeCommits", "true") \
        .table("db_cata.silver.products")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ###**Streaming View**

# COMMAND ----------

@dlt.view()

def DimProducts_view():
    df = spark.readStream \
        .format("delta") \
        .option("skipChangeCommits", "true") \
        .table("Live.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim Products**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------


dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProducts_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2
)