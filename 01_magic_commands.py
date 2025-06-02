# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook 1: Magic Commands

# COMMAND ----------

print("Notebook 1: magic commands")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Notebook 1: magic commands"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a customized dataframe using my own data

# COMMAND ----------

data = [(1, "john", 45), (2, "mary", 34), (3, "peter", 27)]
schema = "id INT, name STRING, marks INT"

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

display(df)
# df.display()