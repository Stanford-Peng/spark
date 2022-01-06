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
                print("saving:")
                df.write.format("delta").mode("ErrorIfExists").save(delta_location)
            # create table via this delta location
            sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format('WWI.'+striped_name.replace(" ", "_"), delta_location)
            print(sql_command)
            spark.sql(sql_command)
        except Exception as e:
            print("Exception:"+str(e))

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


import pyspark.sql.functions as f
df_original = spark.sql("SELECT * FROM wwi.sales_order")
df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).show()

# COMMAND ----------

from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shift the row where CustomerPurchaseOrderNumber is date -- error

# COMMAND ----------

#ERROR 1
import pyspark.sql.functions as f
import pandas
df_original = spark.sql("SELECT * FROM wwi.sales_order")
# df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).shift(periods=-1, axis="columns")
df_error = df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5)
# prevent lazy loading
# df_error.count() not working
pddf = df_error.toPandas()
# print(list(pddf.columns))
pddf1 = pddf.loc[:,"OrderID":"BackorderOrderID":1]
pddf2 = pddf.loc[:,"OrderDate"::1].shift(periods=-1, axis="columns")
print(pddf1)
print(pddf2)
spark.sql("DELETE FROM wwi.sales_order where length(CustomerPurchaseOrderNumber) > 5")
# df_deleted = df_original.where(f.length(f.col("CustomerPurchaseOrderNumber")) <= 5)
# print(df_error.schema)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
df_concat = pandas.concat([pddf1, pddf2], axis=1)
# df_concat.columns
spdf_concat = pandas_to_spark(df_concat)
# df_result = df_deleted.union(spdf_concat)
spdf_concat.write.mode("append").saveAsTable("wwi.sales_order")

# df_error.toPandas().shift(periods=-1, axis="columns")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.sales_order where OrderId = 73596

# COMMAND ----------

# %sql
select * from wwi.sales_order limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.sales_order where length(CustomerPurchaseOrderNumber) > 6;
# MAGIC -- best way is to delete since the last column is not read anyway

# COMMAND ----------

# MAGIC %sql
# MAGIC -- error in salesCustomer
# MAGIC select * from WWI.sales_customers limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from WWI.sales_customers where DeliveryLocation not like 'POINT%';

# COMMAND ----------

df_original = spark.sql("SELECT * FROM wwi.sales_customers")
# df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).shift(periods=-1, axis="columns")
df_error = spark.sql("select * from WWI.sales_customers where DeliveryLocation not like 'POINT%';")
# prevent lazy loading
# df_error.count() not working
pddf = df_error.toPandas()
# print(list(pddf.columns))
pddf1 = pddf.loc[:,:"DeliveryAddressLine2":1]
pddf2 = pddf.loc[:,"DeliveryPostalCode"::1].shift(periods=-1, axis="columns")
print(pddf1)
print(pddf2)
spark.sql("DELETE FROM wwi.sales_customers where DeliveryLocation not like 'POINT%'")
# df_deleted = df_original.where(f.length(f.col("CustomerPurchaseOrderNumber")) <= 5)
# print(df_error.schema)
# Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")
df_concat = pandas.concat([pddf1, pddf2], axis=1)
# df_concat.columns
spdf_concat = pandas_to_spark(df_concat)
spdf_concat.show()
# df_result = df_deleted.union(spdf_concat)
spdf_concat.write.mode("append").saveAsTable("wwi.sales_customers")
# load table one by one is actaully better

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from WWI.sales_customers limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from WWI.sales_customers where CustomerID=1000

# COMMAND ----------


