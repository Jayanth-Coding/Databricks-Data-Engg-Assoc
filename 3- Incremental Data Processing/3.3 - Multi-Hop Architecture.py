# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.github.com/Jayanth-Coding/Databricks-Data-Engg-Assoc/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files=dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(
    spark.readStream.format("cloudFiles") 
    .option("cloudFiles.format", "parquet") 
    .option("cloudFiles.schemaLocation", f"{dataset_bookstore}/checkpoints/orders_raw_checkpoint") 
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

display(spark.read.table("orders_raw_temp"), checkpointLocation = f"{dataset_bookstore}/tmp/checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view orders_temp as
# MAGIC select *, input_file_name() as file_name, date_format(current_timestamp(), 'yyyyMMdd') as arrival_time
# MAGIC from orders_raw_temp

# COMMAND ----------

spark.conf.set("spark.sql.streaming.checkpointLocation", f"{dataset_bookstore}/mnt/workshop/checkpoints/orders-temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from orders_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

(
    spark.read.table("orders_temp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{dataset_bookstore}/checkpoints/orders-bronze")
    .outputMode("append")
    .table("orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(
    spark.read.format("json")
    .load(f"{dataset_bookstore}/customers-json")
    .createOrReplaceTempView("_customers_lookup")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select profile:address:street from _customers_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC     SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name, 
# MAGIC     cast(from_unixtime(order_timestamp, 'yyyy-mm-dd hh:mm:ss') as timestamp) order_timestamp, books
# MAGIC     from orders_temp O
# MAGIC     inner join _customers_lookup C
# MAGIC     on O.customer_id = C.customer_id
# MAGIC     where quantity>0
# MAGIC )
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_enriched_tmp

# COMMAND ----------

(
    spark.table("orders_enriched_tmp")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{dataset_bookstore}/checkpoints/orders-silver")
    .outputMode("append")
    .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver 

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
  print(s.name, s.id)
  s.stop()

# COMMAND ----------


