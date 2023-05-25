-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

DROP TABLE IF EXISTS orders


-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM  orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Table

-- COMMAND ----------

CREATE or REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Another Method For Overwriting
-- MAGIC `INSERT OVERWRITE (tableName)
-- MAGIC   (Query)`
-- MAGIC there are a few differences between this method and the replace method:
-- MAGIC - can only override an existing table
-- MAGIC - data in old table can only be replaced by data in the query
-- MAGIC - can only override records with data that matches table schema

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

CREATE or REPLACE TEMP VIEW customers_updates AS
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Complete a Merge Operation 
-- MAGIC this uses deletes, updates, and inserts all in one file

-- COMMAND ----------

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN 
  UPDATE SET email = u.email, updated = u.updated 
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merge Books 
-- MAGIC using a CSV file

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
  (book_id STRING, title STRING, author STRING, category STRING, price FLOAT)
USING CSV
OPTIONS (
  path ="${dataset.bookstore}/books-csv-new",
  header= "true",
  delimiter = ";"
)

-- COMMAND ----------

SELECT * FROM  books_updates

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC merge book_updates into books only if the book is computer science

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN
  INSERT *

-- COMMAND ----------

SELECT *,
input_file_name() source_file
FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv
(book_id STRING, title STRING, author STRING, category STRING, price FLOAT)
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

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *

-- COMMAND ----------


