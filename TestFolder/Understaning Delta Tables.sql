-- Databricks notebook source
CREATE TABLE employees
(id INT, name STRING, salary DOUBLE );

-- COMMAND ----------

INSERT INTO employees
VALUES
(1, 'Adam', 3500.0),
(2, 'Kyle', 1000.0),
(3, 'Antara', 10000.0),
(4, 'Draco', 7500.0),
(5, 'Arnold', 103500.0),
(6, 'Upa', 500.0)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name Like 'Ant%'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------


