# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employees(
# MAGIC   id INT,
# MAGIC   name VARCHAR(200),
# MAGIC   salary DOUBLE,
# MAGIC   company VARCHAR(200)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.employees VALUES
# MAGIC (123, 'abc', 10, 'A'),
# MAGIC (234, 'bcd', 20, 'B'),
# MAGIC (345, 'cde', 50, 'A'),
# MAGIC (456, 'deg', 50, 'A'),
# MAGIC (567, 'egh', 10, 'C'),
# MAGIC (678, 'ghi', 30, 'A'),
# MAGIC (789, 'hit', 56, 'B'),
# MAGIC (890, 'its', 60, 'B'),
# MAGIC (901, 'tsa', 48, 'A')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL default.employees|

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE default.employees
# MAGIC SET salary = salary*1.2
# MAGIC WHERE company LIKE '%C%'

# COMMAND ----------


