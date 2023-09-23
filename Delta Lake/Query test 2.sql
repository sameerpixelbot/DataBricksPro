-- Databricks notebook source
SELECT * FROM books

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT
customer_id,
profile:first_name,
profile:address:country
FROM customers

-- COMMAND ----------

SELECT profile FROM customers LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW parsed_customers AS
SELECT
customer_id,
from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}') ) AS profile_struct
FROM customers

-- COMMAND ----------

SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT
customer_id,
profile_struct.address.city,
profile_struct.first_name,
profile_struct.gender
FROM parsed_customers

-- COMMAND ----------

SELECT
customer_id,
profile_struct.address.*,
profile_struct.*
FROM parsed_customers

-- COMMAND ----------

DESCRIBE orders

-- COMMAND ----------

SELECT
order_id,
customer_id,
explode(books) AS book
FROM orders

-- COMMAND ----------

SELECT
customer_id,
collect_set(order_id) AS orders_set,
collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

SELECT
customer_id,
collect_set(order_id) AS orders_set,
array_distinct(flatten(collect_set(books.book_id))) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

SELECT
customer_id,
collect_set(order_id) AS orders_set,
forall(flatten(collect_set(books.book_id)), book -> book LIKE 'B10' ) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_enriched AS
SELECT
*
FROM(
  SELECT
  order_id,
  order_timestamp,
  customer_id,
  quantity,
  total,
  explode(books) AS book
  FROM orders
) o
INNER JOIN books b ON
o.book.book_id = b.book_id

-- COMMAND ----------

SELECT * FROM orders_enriched

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (SELECT
customer_id,
book_id,
book.quantity AS quantity
FROM orders_enriched)
PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
)

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT
order_id,
books,
filter(books, book -> book.quantity >=2) AS multiple_copies
FROM orders

-- COMMAND ----------

SELECT * FROM(SELECT
order_id,
books,
filter(books, book -> book.quantity >=2) AS multiple_copies
FROM orders)
WHERE size(multiple_copies) > 0

-- COMMAND ----------

SELECT
order_id,
books,
transform(books, book -> CAST(book.subtotal/book.quantity AS INT)) AS books_prices
FROM orders

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING
RETURN concat("https://www.",split(email, "@")[1])

-- COMMAND ----------

SELECT
customer_id,
get_url(email) AS domain
FROM customers

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN
CASE
WHEN email LIKE "%.com" THEN "Commercial business"
WHEN email LIKE "%.org" THEN "Non-profits organization"
WHEN email LIKE "%.edu" THEN "Educational institution"
ELSE concat("Unknown extension for domain: ", split(email, "@")[1])
END;

-- COMMAND ----------

SELECT
customer_id,
site_type(email) AS domain_category
FROM customers

-- COMMAND ----------


