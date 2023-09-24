-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Databricks notebook source
-- MAGIC def path_exists(path):
-- MAGIC   try:
-- MAGIC     dbutils.fs.ls(path)
-- MAGIC     return True
-- MAGIC   except Exception as e:
-- MAGIC     if 'java.io.FileNotFoundException' in str(e):
-- MAGIC       return False
-- MAGIC     else:
-- MAGIC       raise
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def download_dataset(source, target):
-- MAGIC     files = dbutils.fs.ls(source)
-- MAGIC
-- MAGIC     for f in files:
-- MAGIC         source_path = f"{source}/{f.name}"
-- MAGIC         target_path = f"{target}/{f.name}"
-- MAGIC         if not path_exists(target_path):
-- MAGIC             print(f"Copying {f.name} ...")
-- MAGIC             dbutils.fs.cp(source_path, target_path, True)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC data_source_uri = "wasbs://course-resources@dalhussein.blob.core.windows.net/datasets/bookstore/v1/"
-- MAGIC dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC def get_index(dir):
-- MAGIC     files = dbutils.fs.ls(dir)
-- MAGIC     index = 0
-- MAGIC     if files:
-- MAGIC         file = max(files).name
-- MAGIC         index = int(file.rsplit('.', maxsplit=1)[0])
-- MAGIC     return index+1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # Structured Streaming
-- MAGIC streaming_dir = f"{dataset_bookstore}/orders-streaming"
-- MAGIC raw_dir = f"{dataset_bookstore}/orders-raw"
-- MAGIC
-- MAGIC def load_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.parquet"
-- MAGIC     print(f"Loading {latest_file} file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_data(all=False):
-- MAGIC     index = get_index(raw_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC # DLT
-- MAGIC streaming_orders_dir = f"{dataset_bookstore}/orders-json-streaming"
-- MAGIC streaming_books_dir = f"{dataset_bookstore}/books-streaming"
-- MAGIC
-- MAGIC raw_orders_dir = f"{dataset_bookstore}/orders-json-raw"
-- MAGIC raw_books_dir = f"{dataset_bookstore}/books-cdc"
-- MAGIC
-- MAGIC def load_json_file(current_index):
-- MAGIC     latest_file = f"{str(current_index).zfill(2)}.json"
-- MAGIC     print(f"Loading {latest_file} orders file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_orders_dir}/{latest_file}", f"{raw_orders_dir}/{latest_file}")
-- MAGIC     print(f"Loading {latest_file} books file to the bookstore dataset")
-- MAGIC     dbutils.fs.cp(f"{streaming_books_dir}/{latest_file}", f"{raw_books_dir}/{latest_file}")
-- MAGIC
-- MAGIC     
-- MAGIC def load_new_json_data(all=False):
-- MAGIC     index = get_index(raw_orders_dir)
-- MAGIC     if index >= 10:
-- MAGIC         print("No more data to load\n")
-- MAGIC
-- MAGIC     elif all == True:
-- MAGIC         while index <= 10:
-- MAGIC             load_json_file(index)
-- MAGIC             index += 1
-- MAGIC     else:
-- MAGIC         load_json_file(index)
-- MAGIC         index += 1
-- MAGIC
-- MAGIC # COMMAND ----------
-- MAGIC
-- MAGIC download_dataset(data_source_uri, dataset_bookstore)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC   spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "parquet")
-- MAGIC   .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
-- MAGIC   .load(f"{dataset_bookstore}/orders-raw")
-- MAGIC   .createOrReplaceTempView("orders_raw_temp")
-- MAGIC )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS
SELECT
*,
current_timestamp() AS arrival_time,
input_file_name() AS source_file
FROM orders_raw_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.table("orders_tmp")
-- MAGIC     .writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
-- MAGIC     .outputMode("append")
-- MAGIC     .table("orders_bronze")
-- MAGIC )

-- COMMAND ----------

DROP VIEW order_tmp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.read
-- MAGIC     .format("json")
-- MAGIC     .load(f"{dataset_bookstore}/customers-json")
-- MAGIC     .createOrReplaceTempView("customers_lookup")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.readStream
-- MAGIC     .table("orders_bronze")
-- MAGIC     .createOrReplaceTempView("orders_bronze_tmp")
-- MAGIC )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_enriched_temp AS
SELECT
order_id,
quantity,
o.customer_id,
c.profile:first_name AS f_name,
c.profile:last_name As l_name,
CAST(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) order_timestamp,
books
FROM orders_bronze_tmp o
INNER JOIN customers_lookup c
ON o.customer_id = c.customer_id
WHERE quantity > 0

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.table("orders_enriched_temp")
-- MAGIC     .writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
-- MAGIC     .outputMode("append")
-- MAGIC     .table("order_silver")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.readStream
-- MAGIC     .table("order_silver")
-- MAGIC     .createOrReplaceTempView("orders_silver_tmp")
-- MAGIC )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW daily_customer_books_tmp AS
SELECT
customer_id,
f_name,
l_name,
date_trunc("DD", order_timestamp) AS order_date,
sum(quantity) AS books_count
FROM orders_silver_tmp
GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.table("daily_customer_books_tmp")
-- MAGIC     .writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
-- MAGIC     .outputMode("complete")
-- MAGIC     .trigger(availableNow = True)
-- MAGIC     .table("daily_customer_books")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data(all = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for s in spark.streams.active:
-- MAGIC     print(f"Stopping stream: {s.id}")
-- MAGIC     print(s)
-- MAGIC     s.stop()
-- MAGIC     s.awaitTermination

-- COMMAND ----------


