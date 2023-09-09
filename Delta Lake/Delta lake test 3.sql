-- Databricks notebook source
CREATE TABLE default.managed_default(
  width INT,
  length INT,
  height INT
)

-- COMMAND ----------

INSERT INTO default.managed_default VALUES
(1, 2, 3),
(2, 3, 4),
(3, 4, 5),
(4, 5, 6),
(5, 6, 7)

-- COMMAND ----------

DESCRIBE EXTENDED default.managed_default

-- COMMAND ----------

CREATE TABLE default.external_default LOCATION 'dbfs:/mnt/demo/external_default' AS SELECT width*2 AS width, length*3 AS length, height*8 AS height FROM default.managed_default



-- COMMAND ----------

SELECT * FROM default.external_default

-- COMMAND ----------

DESCRIBE EXTENDED default.external_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse

-- COMMAND ----------

CREATE DATABASE new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

DROP DATABASE new_default

-- COMMAND ----------


