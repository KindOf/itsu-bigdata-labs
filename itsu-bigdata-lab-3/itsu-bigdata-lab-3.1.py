# Databricks notebook source
import os
import json
import requests
from azure.storage.blob import BlobServiceClient

STORAGE_CONNECTION_STRING= os.environ.get("STORAGE_CONNECTION_STRING")
response = requests.get("https://jsonplaceholder.typicode.com/todos/")

if response.status_code == 200:
    data = response.json()
    data_str = json.dumps(data)

    connection_string = STORAGE_CONNECTION_STRING

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = "todos"
    container_client = blob_service_client.get_container_client(container_name)

    blob_name = "todos.json"

    container_client.upload_blob(name=blob_name, data=data_str)
else:
    print("Error: Failed to retrieve data from the API endpoint")

# COMMAND ----------


