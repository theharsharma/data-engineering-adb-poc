# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks File System (DBFS)

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

app_secret = dbutils.secrets.get(scope="harsh-app-secret", key="adb-app-secret")
app_client_id = dbutils.secrets.get(scope="harsh-app-secret", key="sp-client-id")
app_tenant_id = dbutils.secrets.get(scope="harsh-app-secret", key="sp-tenant-id")

endpoint = f"https://login.microsoftonline.com/{app_tenant_id}/oauth2/token"

spark.conf.set("fs.azure.account.auth.type.stadbdatalakeharsh.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stadbdatalakeharsh.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stadbdatalakeharsh.dfs.core.windows.net", app_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.stadbdatalakeharsh.dfs.core.windows.net", app_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stadbdatalakeharsh.dfs.core.windows.net", endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.fs()**

# COMMAND ----------

# use this utility to test the connection
dbutils.fs.ls("abfss://source@stadbdatalakeharsh.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets**

# COMMAND ----------

dbutils.widgets.text("name_param", "Harsh")

# COMMAND ----------

var = dbutils.widgets.get("name_param")
print(var)

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets**

# COMMAND ----------

dbutils.secrets.list(scope="harsh-app-secret")
dbutils.secrets.get(scope="harsh-app-secret", key="adb-app-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading/ Data Ingestion

# COMMAND ----------

df_sales = spark.read.format("csv")\
                      .option("header", "true")\
                      .option("inferSchema", "true")\
                      .load("abfss://source@stadbdatalakeharsh.dfs.core.windows.net/")

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Transformations using PySpark**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales.withColumn("Item_Type", split(col("Item_Type"), " ")).display()

# COMMAND ----------

df_sales.withColumn("remark flag", lit(var)).display()

# COMMAND ----------

df_sales.withColumn("Item_Visibility", col("Item_Visibility").cast(StringType())).display()