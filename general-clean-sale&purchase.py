# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.datalaketeam3.dfs.core.windows.net",
   "4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==")

# COMMAND ----------

!pip install azure-storage-blob --upgrade

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
connect_str = 'DefaultEndpointsProtocol=https;AccountName=datalaketeam3;AccountKey=4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
try:
    curate_container_client = blob_service_client.get_container_client("curate")
    curate_container_client.delete_container()
    blob_service_client.create_container('curate')
except:
    blob_service_client.create_container('curate')
curate_container_client = blob_service_client.get_container_client("curate")
print(curate_container_client.__class__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data and condense the files into table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database WWI CASCADE;
# MAGIC create database WWI;

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import re
connect_str = 'DefaultEndpointsProtocol=https;AccountName=datalaketeam3;AccountKey=4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
raw_container_client = blob_service_client.get_container_client("raw")
curate_container_client = blob_service_client.get_container_client("curate")
blobs_list = raw_container_client.list_blobs()
# for i in [blob['name'] for blob in curate_container_client.list_blobs()]: 
#     print(i)
stored_delta = []
for blob in blobs_list:
    if blob.name.endswith(".csv"):
        print("start processing blob:" + blob.name)
        try: 
            df = spark.read.option("delimiter", ",").option("header", "true").option("escape","\"").option("multiline",True).csv("abfss://raw@datalaketeam3.dfs.core.windows.net/" + blob.name)
            #changing the df into delta format/table and save into curate from now on
#           regex = re.sub("")
            striped_name = re.sub("s?_part\d.csv","",blob.name).replace(".csv", "")
            delta_location='abfss://curate@datalaketeam3.dfs.core.windows.net/' + striped_name
            print("stripped:"+striped_name)
            if striped_name in [blob['name'] for blob in curate_container_client.list_blobs()]:
                print("appending:")
                df.write.format("delta").mode("append").save(delta_location)
            else:
#                 print("saving:")
                df.write.format("delta").mode("ErrorIfExists").save(delta_location)
            # create table via this delta location
            sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
            print(sql_command)
            spark.sql(sql_command)
        except Error as e:
            print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Sales

# COMMAND ----------

#remove dulicate rows except OrderID
df_original = spark.sql("SELECT * FROM WWI.sales_order")
import pyspark.sql.functions as f
#df.show()
df_cleaned = df_original.dropDuplicates(subset=[col for col in df_original.columns if col!='OrderID'])
df_cleaned.groupBy([col for col in df_original.columns if col!='OrderID']).agg(f.count("*").alias("count")).where(f.col("count") >1).show(truncate=False)
df_cleaned.write.mode("overwrite").saveAsTable("WWI.sales_order")

# COMMAND ----------

df_original = spark.sql("SELECT * FROM sales_order")
list_c = df_original.columns.remove("OrderID")
print(df_original.columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerPurchaseOrderNumber, count(*) as dups
# MAGIC FROM WWI.sales_order
# MAGIC group by CustomerPurchaseOrderNumber
# MAGIC having count(*) > 1

# COMMAND ----------

df_original = spark.sql("SELECT * FROM sales_order")
[col for col in df_original.columns if col!='OrderID']

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.warehouse_stockitemtransaction;

# COMMAND ----------

#ERROR 1
import pyspark.sql.functions as f
df_original = spark.sql("SELECT CustomerPurchaseOrderNumber FROM wwi.sales_order")
df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).show()

# COMMAND ----------

# error in salesCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count, Location from wwi.application_cities group by Location having count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_cities where Location = 'POINT (-66.1287777 18.2185674)' 

# COMMAND ----------

df = spark.read.option("delimiter", ",").option("header", "true").option("escape","\"").csv("abfss://raw@datalaketeam3.dfs.core.windows.net/" + "application_people.csv")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_people;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_systemparameters limit 10;

# COMMAND ----------

df = spark.read.option("delimiter", ",").option("header", "true").option("escape","\"").option("multiline",True).csv("abfss://raw@datalaketeam3.dfs.core.windows.net/" + "application_systemparameters.csv")
df.show()

# COMMAND ----------


