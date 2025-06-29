# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


dbutils.widgets.text("init_load_flag", "0", "Initial Load Flag")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

df = spark.read.format("delta").load("abfss://silver@adlsdbetenis.dfs.core.windows.net/customers")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df = spark.sql("select * from db_cata.silver.customers")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Removing Duplicates**

# COMMAND ----------

df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Seperating old vs new records**

# COMMAND ----------

if init_load_flag ==0:
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date
                        from db_cata.gold.DimCustomers''' )

else:

      df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date,0 update_date
                       from db_cata.silver.customers where 1=0''')




# COMMAND ----------

df_old.limit(5).display()

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "DimCustomerKey_new")\
    .withColumnRenamed("customer_id", "customer_id_new")\
    .withColumnRenamed("create_date", "create_date_new")\
    .withColumnRenamed("update_date", "update_date_new")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Join with old records**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.customer_id_new, 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_new = df_join.filter(df_join.DimCustomerKey_new.isNull())
df_new.limit(5).display()

# COMMAND ----------

df_old = df_join.filter(df_join.DimCustomerKey_new.isNotNull())
df_old.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_old**

# COMMAND ----------

# dropping all the columns which are not required

df_old = df_old.drop("customer_id_new", "update_date_new")

# Renaming "DimCustomerKey_new" to "DimCustomerKey"

df_old = df_old.withColumnRenamed("DimCustomerKey_new", "DimCustomerKey")

# Renaming "create_date_new" to "create_date"

df_old = df_old.withColumnRenamed("create_date_new", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# Recreating "update_date" column with current timestamp

df_old = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing df_new**

# COMMAND ----------

# dropping all the columns which are not required

df_new = df_new.drop("customer_id_new", "DimCustomerKey_new","update_date_new","create_date_new")


# Recreating "update_date","create_date" columns with current timestamp
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())



# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Surrogate key - from 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))
df_new.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Adding Max Surrogate key**

# COMMAND ----------

if init_load_flag ==1:

  max_surrogate_key = 0

else:

  df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from db_cata.gold.DimCustomers")

### **Converting df_maxsur to max_surrogate_key variable**

  max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union of df_old and df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD Type-1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("db_cata.gold.DimCustomers"):

  dlt_obj = DeltaTable.forPath(spark,"abfss://gold@adlsdbetenis.dfs.core.windows.net/DimCustomers")

  dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey=src.DimCustomerKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

 

else:
   df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path","abfss://gold@adlsdbetenis.dfs.core.windows.net/DimCustomers").saveAsTable("db_cata.gold.DimCustomers")
  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.gold.DimCustomers

# COMMAND ----------

