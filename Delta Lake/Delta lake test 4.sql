-- Databricks notebook source
CREATE TABLE phone(
  id INT,
  name STRING,
  brand STRING,
  year INT
)

-- COMMAND ----------

INSERT INTO default.phone VALUES
(1, 'iPhone 14', 'Apple', 2022),
(2, 'iPhone 12', 'Apple', 2020),
(3, 'iPhone 11', 'Apple', 2019),
(4, 'iPhone 13', 'Apple', 2021),
(5, 'Nothing phone 1', 'Nothing', 2021),
(6, 'Galaxy S20', 'Apple', 2020),
(7, 'Galaxy S22', 'Apple', 2022),
(8, 'Pixel 7', 'Apple', 2022),
(9, 'Nothing phone 2', 'Nothing', 2022)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

/*UPDATE phone SET
brand = 'Google'
WHERE lower(name) like 'pix%'*/

-- COMMAND ----------

CREATE VIEW apple_phone_view AS
SELECT * FROM phone
WHERE lower(brand) = 'apple'

-- COMMAND ----------

SELECT * FROM apple_phone_view

-- COMMAND ----------

CREATE TEMPORARY VIEW brand_view AS
SELECT DISTINCT brand FROM phone

-- COMMAND ----------

SELECT * FROM brand_view

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_post_covid_phone_view AS
SELECT * FROM phone
WHERE year >2020
ORDER BY year DESC

-- COMMAND ----------

SELECT * FROM global_temp.global_post_covid_phone_view

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

DROP TABLE employees;
DROP TABLE external_default;
DROP TABLE managed_default;
DROP TABLE phone;

DROP VIEW apple_phone_view;

-- COMMAND ----------


