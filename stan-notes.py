# Databricks notebook source
#Libraries required
import pyspark.sql.functions as f
!pip install azure-storage-blob --upgrade

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalaketeam3.dfs.core.windows.net",
   "4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==")

# COMMAND ----------

#import uuid
#from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
connect_str = 'DefaultEndpointsProtocol=https;AccountName=datalaketeam3;AccountKey=4vT82BPbBT8hnSf4asSb9yHSsgH/ZFqURQBxzV9fNxa1IAiZdtjs04w7KZqe3LOV/byR/C3+sBAmBvt8AUs2SA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Create a unique name for the container
container_name = "raw"

container_client = ContainerClient.from_connection_string(connect_str  , container_name)

blobs_list = container_client.list_blobs()
for blob in blobs_list:
    if blob.name.endswith(".csv"):
        print(blob.name + '\n')
        try: 
            df = spark.read.option("delimiter", ",").option("header", "true").csv("abfss://raw@datalaketeam3.dfs.core.windows.net/" + blob.name)
    #         display(df)
            #changing the df into delta format/table
        
        
        
        
        
            df.write.format("delta").mode("overwrite").save("abfss://presentation@datalaketeam3.dfs.core.windows.net/" + blob.name)
            delta_location='abfss://presentation@datalaketeam3.dfs.core.windows.net/' + blob.name
            sql_command = "CREATE TABLE if not exists {} USING DELTA LOCATION '{}'".format(blob.name.replace(" ", "").replace(".csv", ""), delta_location)
            print(sql_command)
            spark.sql(sql_command)
    #         break
        except Error as e:
            print(e)
            


# COMMAND ----------

# %sql

# SELECT  * from default.sales_order
# order by OrderDate desc

# COMMAND ----------

# %sql
# WITH CTE AS(
#    SELECT StateProvinceID ,  ROW_NUMBER() OVER(PARTITION BY StateProvinceID ORDER BY StateProvinceID) AS RN
#    FROM default.dishi
#    --where CustomerPurchaseOrderNumber IS NOT NULL
# )
# DELETE FROM CTE WHERE RN > 1
# --SELECT * FROM CTE



# COMMAND ----------

# %sql

# SELECT  * from default.dishi;


# COMMAND ----------

#  %sql
# -- # WITH CTE AS(
# -- #    SELECT CustomerPurchaseOrderNumber ,  ROW_NUMBER() OVER(PARTITION BY CustomerPurchaseOrderNumber ORDER BY CustomerPurchaseOrderNumber) AS RN
# -- #    FROM default.sales_order
# -- #    where CustomerPurchaseOrderNumber IS NOT NULL
# -- # )
# -- # DELETE FROM CTE WHERE RN > 1
# -- # --SELECT * FROM CTE


# ALTER TABLE default.sales_order
# ADD COLUMNS Row_Number DECIMAL(10,2);



# COMMAND ----------

# %sql

# UPDATE sales_order
# SET Row_Number = 
# --INSERT INTO sales_order (Row_Number)
# (SELECT ROW_NUMBER() OVER(PARTITION BY CustomerPurchaseOrderNumber ORDER BY CustomerPurchaseOrderNumber) AS Row_Number
# FROM sales_order)


# COMMAND ----------

# %sql
# select CustomerPurchaseOrderNumber, count(*)
# from sales_order
# group by CustomerPurchaseOrderNumber
# having count(*) > 1;

# COMMAND ----------

# %sql
# -- select row_number from default.sales_order;
# with CTE as (SELECT * , ROW_NUMBER() OVER(PARTITION BY CustomerPurchaseOrderNumber ORDER BY CustomerPurchaseOrderNumber) AS row_number
# FROM sales_order) 
# --with joined as (select * from cte right join sales_order s on cte.CustomerPurchaseOrderNumber = s.CustomerPurchaseOrderNumber)
# DELETE FROM CTE
# WHERE row_number > 1;

# COMMAND ----------

# # sql= r"delete from sales_order where CustomerPurchaseOrderNumber in ( SELECT CustomerPurchaseOrderNumber , ROW_NUMBER() OVER(PARTITION BY CustomerPurchaseOrderNumber ORDER BY CustomerPurchaseOrderNumber) FROM default.sales_order limit"
# limit = spark.sql("SELECT CustomerPurchaseOrderNumber, count(*) as dups FROM sales_order group by CustomerPurchaseOrderNumber having count(*) > 1")
# print(limit);
# for row in limit.collect():
#     sql_command="delete from sales_order where CustomerPurchaseOrderNumber={}".format(row['CustomerPurchaseOrderNumber'])
#     print(sql_command)
#     spark.sql(sql_command)
#     break;
# # limit.rdd.map(lambda row: spark.sql("delete from sales_order where CustomerPurchaseOrderNumber={} limit {}".format(row['CustomerPurchaseOrderNumber'],row[dups]-1)))
    

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW COLUMNS IN default.sales_order;
# MAGIC SELECT CustomerPurchaseOrderNumber, count(*) as dups
# MAGIC FROM WWI.sales_order
# MAGIC group by CustomerPurchaseOrderNumber
# MAGIC having count(*) > 1

# COMMAND ----------


df_original = spark.sql("SELECT * FROM sales_order")

#df.show()
df_cleaned = df.dropDuplicates(['CustomerPurchaseOrderNumber'])
df_cleaned.groupBy('CustomerPurchaseOrderNumber').count().select('CustomerPurchaseOrderNumber', f.col('count').alias('count')).show()
df_cleaned.write.mode("overwrite").saveAsTable("sales_order")



# COMMAND ----------

# %sql
# describe detail sales_order;

# COMMAND ----------

# %sql
# df = sqlContext.table("sales_order")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_customers;
# MAGIC SHOW COLUMNS IN default.sales_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 1
# MAGIC --SHOW COLUMNS IN default.sales_order;
# MAGIC SELECT CustomerID, count(*) as dups
# MAGIC FROM sales_customers
# MAGIC group by CustomerID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 2
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE CustomerID IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 3
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE CustomerCategoryID NOT IN
# MAGIC (select CustomerCategoryID from sales_customercategories)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 4
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE BuyingGroupID NOT IN
# MAGIC (select BuyingGroupID from sales_buyinggroups)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 5
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE PrimaryContactPersonID NOT IN --AlternateContactPersonID also
# MAGIC (select PersonID from application_people)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 6
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE DeliveryMethodID NOT IN
# MAGIC (select DeliveryMethodID from application_paymentmethods)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 7
# MAGIC SELECT *
# MAGIC FROM sales_customers
# MAGIC WHERE DeliveryCityID NOT IN --PostalCityID also
# MAGIC (select CityID from application_cities)

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC --select DeliveryPostalCode from sales_customers;
# MAGIC --SHOW COLUMNS IN default.application_systemparameters;
# MAGIC select distinct DeliveryPostalCode from application_systemparameters

# COMMAND ----------

# CHECK 8
df_original = spark.sql("SELECT AccountOpenedDate FROM sales_customers")

df = df_original.withColumn("first_n_char", df_original.AccountOpenedDate.substr(1,10))
df.select('first_n_char').distinct().show(df.count(),False)

# COMMAND ----------


df_original = spark.sql("SELECT * FROM sales_customers")

#df.show()
df_cleaned = df.dropDuplicates(['CustomerPurchaseOrderNumber'])
df_cleaned.groupBy('CustomerPurchaseOrderNumber').count().select('CustomerPurchaseOrderNumber', f.col('count').alias('count')).show()
df_cleaned.write.mode("overwrite").saveAsTable("sales_order")



# COMMAND ----------

# MAGIC %md
# MAGIC # Stan Analysis on Table sales_buyinggroup and sales_customer_category

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %sql select * from sales_buyinggroups;
# MAGIC -- describe sales_buyinggroups;
# MAGIC -- only two rows in total
# MAGIC -- convert string to date

# COMMAND ----------

# MAGIC %sql select * from sales_customercategories;
# MAGIC --only 8 rows
# MAGIC --what is last_edited_by

# COMMAND ----------

# MAGIC %sql select * from application_people;
# MAGIC --is last_edited_by personid here

# COMMAND ----------

# MAGIC %sql describe table sales_customercategories;
# MAGIC --Do we need to covert the data type to be date

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sales_customers customers join sales_customercategories categories on customers.CustomerCategoryID = categories.CustomerCategoryID limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select count(*) as freq, CustomerCategoryID from sales_customers group by CustomerCategoryID) customers left join sales_customercategories categories on customers.CustomerCategoryID = categories.CustomerCategoryID;
# MAGIC -- category 1,2,8 Agent Wholesaler General Retailer has never been used

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders;

# COMMAND ----------

df_original = spark.sql("SELECT * FROM sales_order")
import pyspark.sql.functions as f
# -- df.show()
# df_original = df_original.dropDuplicates(['CustomerPurchaseOrderNumber'])
df_original.groupBy('CustomerPurchaseOrderNumber').count().select('CustomerPurchaseOrderNumber', f.col('count').alias('count')).show()
# -- df_cleaned.write.mode("overwrite").saveAsTable("sales_order")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count FROM sales_order group by CustomerPurchaseOrderNumber having count > 1

# COMMAND ----------

df_original = spark.sql("SELECT * FROM sales_order")
import pyspark.sql.functions as f
df = df_original.filter(df_original.OrderDate > "2015-05")
df.show()
df.write.mode("overwrite").saveAsTable("sales_order")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_order where OrderDate < "2015-05"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail wwi.sales_order

# COMMAND ----------


