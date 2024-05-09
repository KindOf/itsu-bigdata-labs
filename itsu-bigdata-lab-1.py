# Databricks notebook source
import os

uri = os.environ.get("MONGO_URI")

# COMMAND ----------

data = spark.read.table("default.movies")
data.write.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.output.uri", uri).option("database", "itsu-bg-lab-1").option("collection","movies").mode("append").save()
