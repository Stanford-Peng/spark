# Databricks notebook source
dbutils.widgets.text("sourceFolder", "","")
dbutils.widgets.text("sourceFile", "","")
source_folder = dbutils.widgets.get("sourceFolder")
source_file = dbutils.widgets.get("sourceFile")
print("Processing {}/{}:".format(source_folder,source_file))
print(source_folder)
print(source_file)

# COMMAND ----------


access_key = dbutils.secrets.get("azure-key-vault", "storage-account-access-key")
spark.conf.set(
    "fs.azure.account.key.datalaketeam3.dfs.core.windows.net",
   access_key)


# COMMAND ----------

try:
    dbutils.fs.mount(
    source = "wasbs://landing@datalaketeam3.blob.core.windows.net",
    mount_point = "/mnt/landing",
    extra_configs = {"fs.azure.account.key.datalaketeam3.blob.core.windows.net":"4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA=="})
except Exception as e:
  print("mounted already")

# COMMAND ----------


!pip install azure-storage-blob --upgrade
!pip install fsspec


# COMMAND ----------

# MAGIC %md
# MAGIC databricks clusters list

# COMMAND ----------

# MAGIC %md
# MAGIC python -m pip install --upgrade pandas

# COMMAND ----------

# !pip3 install lxml
# !pip install pyspark.pandas

# COMMAND ----------

# MAGIC %md
# MAGIC spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.azure.account.key.datalaketeam3.blob.core.windows.net", "4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop database  WWI CASCADE;
# MAGIC create database if not exists WWI;

# COMMAND ----------


# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# import re
# import pandas as pd

# connect_str = 'DefaultEndpointsProtocol=https;AccountName=datalaketeam3;AccountKey=4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==;EndpointSuffix=core.windows.net'
# blob_service_client = BlobServiceClient.from_connection_string(connect_str)
# landing_container_client = blob_service_client.get_container_client("landing")
# curate_container_client = blob_service_client.get_container_client("curate")

# # df = pd.read_xml("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml")
# # df = spark.read.format("com.databricks.spark.xml").options(rowTag="Row",inferSchema=True).load("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml")
# if source_file.endswith(".xml"):
#     print("start processing blob:" + source_file)
#     try:
#       df = spark.read.format("com.databricks.spark.xml").options(rowTag="Row", rootTag="Table").option("multiline",True).load("/mnt/landing/" + source_file) 
#       striped_name = re.sub("s?_part\d.xml","",source_file).replace(".xml", "")
#       delta_location='abfss://curate@datalaketeam3.dfs.core.windows.net/' + striped_name
#       print("Processing stripped:"+striped_name)
#       if striped_name in [blob['name'] for blob in curate_container_client.list_blobs()]:
#           print("appending:")
#           df.write.format("delta").mode("append").save(delta_location)
#       else:
#           print("saving:")
#           df.write.format("delta").mode("ErrorIfExists").save(delta_location)
#       # create table via this delta location
#       sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
#       print(sql_command)
#       spark.sql(sql_command)
#     except Exception as e:
#         print("Exception:"+str(e))
# elif source_file.endswith(".json"):
#     print("start processing blob:" + source_file)
#     try:
#         df = spark.read.format("json").option("multiline","true").load("/mnt/landing/" + source_file) 
#         striped_name = re.sub("s?_part\d.json","",source_file).replace(".json", "")
#         delta_location='abfss://curate@datalaketeam3.dfs.core.windows.net/' + striped_name
#         print("Processing stripped:"+striped_name)
#         if striped_name in [blob['name'] for blob in curate_container_client.list_blobs()]:
#             print("appending:")
#             df.write.format("delta").mode("append").save(delta_location)
#         else:
#             print("saving:")
#             df.write.format("delta").mode("ErrorIfExists").save(delta_location)
#         # create table via this delta location
#         sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
#         print(sql_command)
#         spark.sql(sql_command)
#     except Exception as e:
#         print("Exception:"+str(e))
# elif source_file.endswith(".csv"):
#     print("start processing blob:" + source_file)
#     try: 
#         df = spark.read.option("delimiter", ",").option("header", "true").option("escape","\"").option("multiline",True).csv("/mnt/landing/" + source_file)
#         #changing the df into delta format/table and save into curate from now on
# #           regex = re.sub("")
#         striped_name = re.sub("s?_part\d.csv","",source_file).replace(".csv", "")
#         delta_location='abfss://curate@datalaketeam3.dfs.core.windows.net/' + striped_name
#         print("stripped:"+striped_name)
#         if striped_name in [blob['name'] for blob in curate_container_client.list_blobs()]:
#             print("appending:")
#             df.write.format("delta").mode("append").save(delta_location)
#         else:
#             print("saving:")
#             df.write.format("delta").mode("ErrorIfExists").save(delta_location)
#         # create table via this delta location
#         sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
#         print(sql_command)
#         spark.sql(sql_command)
#     except Exception as e:
#         print("Exception:"+str(e))
  



# COMMAND ----------

# # dbutils.library.installPyPI("pandas", "1.3.4")
# import pandas as pd
# import sys
# df=pd.read_xml("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml")
# print(df.head())
# df = spark.read.format("json").option("multiline","true").load("/mnt/landing/" + "warehouse_packagetypes.json")
# df.show()

# COMMAND ----------

# with open("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml") as f:
#   string_xml = f.read()
# print(string_xml)

# COMMAND ----------

def read_files(df, source_file,curate_container_client,extension):
      striped_name = re.sub("s?_part\d"+extension,"",source_file).replace(extension, "")
      delta_location='abfss://curate@datalaketeam3.dfs.core.windows.net/' + striped_name
      print("Processing stripped:"+striped_name)
      if striped_name in [blob['name'] for blob in curate_container_client.list_blobs()]:
          print("appending:")
          df.write.format("delta").mode("append").save(delta_location)
      else:
          print("saving:")
          df.write.format("delta").mode("ErrorIfExists").save(delta_location)
      # create table via this delta location
      sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
      print(sql_command)
      spark.sql(sql_command)

# COMMAND ----------


from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import re
import pandas as pd

connect_str = 'DefaultEndpointsProtocol=https;AccountName=datalaketeam3;AccountKey=4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
landing_container_client = blob_service_client.get_container_client("landing")
curate_container_client = blob_service_client.get_container_client("curate")

# df = pd.read_xml("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml")
# df = spark.read.format("com.databricks.spark.xml").options(rowTag="Row",inferSchema=True).load("abfss://landing@datalaketeam3.dfs.core.windows.net/" + "warehouse_stockitems.xml")
if source_file.endswith(".xml"):
    print("start processing blob:" + source_file)
    try:
      df = spark.read.format("com.databricks.spark.xml").options(rowTag="Row", rootTag="Table",inferSchema=False).option("multiline",True).load("/mnt/landing/" + source_file) 
      read_files(df, source_file,curate_container_client,extension='.xml')
    except Exception as e:
        raise Exception("Pipeline: Failed to process xml file {source_file}".format)
elif source_file.endswith(".json"):
    print("start processing blob:" + source_file)
    try:
        df = spark.read.format("json").option("multiline","true").load("/mnt/landing/" + source_file) 
        read_files(df, source_file,curate_container_client,extension='.json')
    except Exception as e:
        raise Exception("Pipeline: Failed to process json file {source_file}".format)
elif source_file.endswith(".csv"):
    print("start processing blob:" + source_file)
    try: 
        df = spark.read.option("delimiter", ",").option("header", "true").option("escape","\"").option("multiline",True).csv("/mnt/landing/" + source_file)
        read_files(df, source_file,curate_container_client,extension='.csv')
    except Exception as e:
        raise Exception("Pipeline: Failed to process csv file {source_file}".format)
  


