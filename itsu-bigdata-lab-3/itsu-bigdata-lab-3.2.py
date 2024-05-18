# Databricks notebook source
# MAGIC %md
# MAGIC ###Define vars:

# COMMAND ----------

import os

STORAGE_KEY = os.environ.get("STORAGE_KEY")
STORAGE_ACC = "itsubigdatalab3"
CONTAINER = "todos"
FILE = "todos.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read blob from storage and convert to DF:

# COMMAND ----------

# Set up the Blob storage account access key in the notebook session conf
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACC}.blob.core.windows.net",
    STORAGE_KEY
)

# Read the blob as a DataFrame
df = spark.read.format("json").load(f"wasbs://{CONTAINER}@{STORAGE_ACC}.blob.core.windows.net/{FILE}")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add DateRegistration column

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn("DateRegistration", F.expr("date_sub(current_date(), cast(rand() * 365 as int))"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save DF into Warehouse

# COMMAND ----------

df.write.format("delta").saveAsTable("todos")
