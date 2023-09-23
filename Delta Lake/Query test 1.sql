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
-- MAGIC files = dbutils.fs.ls(f'{dataset_bookstore}/orders')
-- MAGIC display(files)

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM JSON.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM CSV.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %fs ls 
-- MAGIC dbfs:/mnt/demo-datasets/bookstore/books-csv

-- COMMAND ----------

CREATE TABLE customers AS
SELECT *, input_file_name() AS source_file FROM JSON.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

DESCRIBE EXTENDED customers

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_view (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";",
  path = "${dataset.bookstore}/books-csv"
)

-- COMMAND ----------

CREATE TABLE books AS
SELECT * FROM books_tmp_view

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

DROP TABLE books_csv

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customers_updates AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json-new`;

-- COMMAND ----------

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT (customer_id, email, updated, profile)
VALUES (u.customer_id, u.email, u.updated, u.profile)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW books_tmp_view (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
)

-- COMMAND ----------

MERGE INTO books b
USING books_tmp_view t
ON b.book_id = t.book_id AND b.title = t.title
WHEN NOT MATCHED AND LOWER(t.category) = 'computer science' THEN
INSERT (book_id, title, author, category)
VALUES (t.book_id, t.title, t.author, t.category)

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------


