# Databricks notebook source
# MAGIC %md
# MAGIC ## Using Delta lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Tranferring csv data from source to destination in parquet/delta format**

# COMMAND ----------

# MAGIC %run "/adb-workspace-primary/02_data_ingestion"

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing data in the destination**

# COMMAND ----------

df_sales.write.format("delta").mode("append").option("path", "abfss://sink@stadbdatalakeharsh.dfs.core.windows.net/sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed vs External Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Database creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS SalesDB

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed delta table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS SalesDB.managed_table_01
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesdb.managed_table_01
# MAGIC VALUES
# MAGIC (1, "John", 90),
# MAGIC (2, "Mary", 80),
# MAGIC (3, "Mike", 88),
# MAGIC (4, "Peter", 55)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.managed_table_01;

# COMMAND ----------

# MAGIC %md
# MAGIC **External Tables**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS SalesDB.external_table_01
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://sink@stadbdatalakeharsh.dfs.core.windows.net/SalesDB/external_table_01" 
# MAGIC -- Ensure the external location is created before executing this query. I created one in the Catalog section using an access connector in Azure

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesdb.external_table_01
# MAGIC VALUES
# MAGIC (1, "John", 90),
# MAGIC (2, "Mary", 80),
# MAGIC (3, "Mike", 88),
# MAGIC (4, "Peter", 55)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.external_table_01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC **Insert**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesdb.external_table_01
# MAGIC VALUES
# MAGIC (5, "John", 90),
# MAGIC (6, "Mary", 80),
# MAGIC (7, "Mike", 88),
# MAGIC (8, "Peter", 55)

# COMMAND ----------

# MAGIC %md
# MAGIC **Delete**

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from salesdb.external_table_01
# MAGIC WHERE id = 8

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY salesdb.external_table_01

# COMMAND ----------

# MAGIC %md
# MAGIC **Time travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- lets restore our data to version 2
# MAGIC RESTORE TABLE salesdb.external_table_01 TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC **Vacuum command**

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM salesdb.external_table_01
# MAGIC -- VACUUM salesdb.external_table_01 RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Table Optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimize**

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE salesdb.external_table_01

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from salesdb.external_table_01

# COMMAND ----------

# MAGIC %md
# MAGIC **ZORDER BY**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE salesdb.external_table_01 ZORDER BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM salesdb.external_table_01

# COMMAND ----------

# MAGIC %md
# MAGIC **Auto Loader**

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "abfss://alsink@stadbdatalakeharsh.dfs.core.windows.net/checkpoint").load("abfss://alsource@stadbdatalakeharsh.dfs.core.windows.net")

# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", "abfss://alsink@stadbdatalakeharsh.dfs.core.windows.net/checkpoint").trigger(processingTime="5 seconds").start("abfss://alsink@stadbdatalakeharsh.dfs.core.windows.net/data")