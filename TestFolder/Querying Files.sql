-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get Files

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Display Files 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Query Against JSON Files
-- MAGIC
-- MAGIC *Use backticks for filepath*

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query all files of JSON type

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query ALl Files In a Directory

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

 SELECT *,
    input_file_name() source_file
  FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query All Text From a File 
-- MAGIC This is useful if there is corrupt data and data needs to be parsed

-- COMMAND ----------

SELECT *,
input_file_name() source_file
 FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

DROP TABLE IF EXISTS books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create A Table From CSV

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
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

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The above table is not well parsed, so create a view to better format data

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------


