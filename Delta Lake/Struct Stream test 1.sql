-- Databricks notebook source
-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.readStream.table("books").createOrReplaceTempView("books_streaming_tmp_vw")
-- MAGIC )

-- COMMAND ----------

SELECT * FROM books_streaming_tmp_vw

-- COMMAND ----------

SELECT
author,
count(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW author_counts_tp_vw AS
SELECT
author,
count(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("author_counts_tp_vw").writeStream.trigger(processingTime = '4 seconds').outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint").table("author_counts")

-- COMMAND ----------

INSERT INTO books VALUES
("B19", "Introduction to Modeling and Simulation", "Mark W. Sponge", "Computer Science", 25),
("B20", "Robot Modeling and Control", "Mark W. Sponge", "Computer Science", 30),
("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------


