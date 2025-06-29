# Databricks notebook source
# MAGIC %md
# MAGIC ### **Schema and Table Creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema db_cata.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists db_cata.silver.customers
# MAGIC using delta
# MAGIC location 'abfss://silver@adlsdbetenis.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists db_cata.silver.products
# MAGIC using delta
# MAGIC location 'abfss://silver@adlsdbetenis.dfs.core.windows.net/products'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists db_cata.silver.orders
# MAGIC using delta
# MAGIC location 'abfss://silver@adlsdbetenis.dfs.core.windows.net/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists db_cata.silver.regions
# MAGIC using delta
# MAGIC location 'abfss://silver@adlsdbetenis.dfs.core.windows.net/regions'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.silver.products

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.silver.regions

# COMMAND ----------

