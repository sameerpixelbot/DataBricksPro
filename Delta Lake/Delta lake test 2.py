# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees
# MAGIC VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE default.employees TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL default.employees

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE default.employees
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM default.employees

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/

# COMMAND ----------


