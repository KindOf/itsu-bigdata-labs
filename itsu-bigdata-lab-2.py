# Databricks notebook source
# MAGIC %md
# MAGIC #Exercise 1: Importing and Displaying Data

# COMMAND ----------

file_path = "/Workspace/data/movies.json"
dbfs_file_path = "dbfs:/data/movies.json"

dbutils.fs.cp("file://" + file_path, dbfs_file_path)

# COMMAND ----------

df = spark.read.json(dbfs_file_path, multiLine=True)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Exercise 2: Data Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ##Identify and Handle Nulls and Remove Duplicates:

# COMMAND ----------

df = df.na.drop(subset=["title"])
df = df.dropDuplicates(["title"])

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Exercise 3: Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Calculated Column:

# COMMAND ----------

from pyspark.sql.functions import when;

df = df.withColumn("is_old", when(df["year"] > 2000, False).otherwise(True))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filter Data

# COMMAND ----------

df = df.filter(df["is_old"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Exercise 4: Writing Data to CosmosDB

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col

df_with_id = df.withColumn("id", monotonically_increasing_id())

df_with_id = df_with_id.withColumn("id", col("id").cast("string"))

# COMMAND ----------

from pyspark.sql import SparkSession
import os

cosmos_key = os.environ.get("COSMOS_KEY")
spark = SparkSession.builder.getOrCreate()

writeConfig = {
    "spark.cosmos.accountEndpoint": "https://itsu-big-data-course.documents.azure.com:443/",
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "lab2",
    "spark.cosmos.container": "movies"
}

df_with_id.write.format("cosmos.oltp").options(**writeConfig).mode("append").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Exercise 5: Querying CosmosDB

# COMMAND ----------

# MAGIC %md
# MAGIC ##CosmosDB Data Readback:

# COMMAND ----------

readConfig = {
    "spark.cosmos.accountEndpoint": "https://itsu-big-data-course.documents.azure.com:443/",
    "spark.cosmos.accountKey": cosmos_key,
    "spark.cosmos.database": "lab2",
    "spark.cosmos.container": "movies"
}

df_cosmos = spark.read.format("cosmos.oltp").options(**readConfig).load()
display(df_cosmos)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SQL Queries:

# COMMAND ----------

df_cosmos.createOrReplaceTempView("cosmos_view");
view = spark.sql("SELECT * FROM cosmos_view WHERE is_old = TRUE")
display(view)
