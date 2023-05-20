# Databricks notebook source
print("Helo, world!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Magic Command 
# MAGIC
# MAGIC - cool
# MAGIC - easy
# MAGIC - fun
# MAGIC
# MAGIC just use ***%*** and a command like `%sql` to add some sql to a cell

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC the `%run` command allows a user to run a different notebook from the notebook using the command

# COMMAND ----------

# MAGIC %run ./includes/SetUp

# COMMAND ----------

print(FUll_NAME)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
display(files)

# COMMAND ----------


