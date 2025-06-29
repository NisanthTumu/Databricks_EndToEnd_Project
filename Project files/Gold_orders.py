# Databricks notebook source
# MAGIC %md
# MAGIC ## **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df= spark.sql("select * from db_cata.silver.orders")

# COMMAND ----------

df.display()

# COMMAND ----------

df_cus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from db_cata.gold.dimcustomers")
df_cus.limit(5).display()

# COMMAND ----------

df_prod = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from db_cata.gold.dimproducts")
df_prod.limit(5).display()

# COMMAND ----------

df_fact = df.join(df_cus, df['customer_id'] == df_cus['dim_customer_id'], 'left').join(df_prod, df['product_id']==df_prod['dim_product_id'],'left')

df_fact.limit(5).display()

# COMMAND ----------

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id','customer_id','product_id')
df_fact_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("db_cata.gold.FactOrders"):
    
    dlt_obj = DeltaTable.forName(spark,"db_cata.gold.FactOrders")

    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id  AND trg.DimCustomerKey = src.DimCustomerKey and trg.DimProductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    
else:
    df_fact_new.write.format("delta")\
        .option("path","abfss://gold@adlsdbetenis.dfs.core.windows.net/FactOrders")\
        .saveAsTable("db_cata.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.gold.FactOrders

# COMMAND ----------

