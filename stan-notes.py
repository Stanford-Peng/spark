# Databricks notebook source
#Libraries required
import pyspark.sql.functions as f
!pip install azure-storage-blob --upgrade

# COMMAND ----------

access_key = dbutils.secrets.get("azure-key-vault", "storage-account-access-key")
spark.conf.set(
    "fs.azure.account.key.datalaketeam3.dfs.core.windows.net",
   access_key)

# COMMAND ----------

try:
    dbutils.fs.mount(
    source = "wasbs://presentation@datalaketeam3.blob.core.windows.net",
    mount_point = "/mnt/presentation",
    extra_configs = {"fs.azure.account.key.datalaketeam3.blob.core.windows.net":access_key})
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dim_city using delta location "/mnt/presentation/dim_city"

# COMMAND ----------

# MAGIC %python
# MAGIC %python
# MAGIC read_format = 'delta'
# MAGIC write_format = 'delta'
# MAGIC load_path = 'dbfs:/user/hive/warehouse/staging.db/datecityitem'
# MAGIC save_path = 'abfss://sales@datalaketeam3.dfs.core.windows.net/'
# MAGIC table_name = 'sales.datecityitem'
# MAGIC 
# MAGIC # Load the data from its source.
# MAGIC transferdf = spark.read.format(read_format).load(load_path)
# MAGIC 
# MAGIC # Write the data to its target.
# MAGIC transferdf.write.format(write_format).save(save_path)
# MAGIC 
# MAGIC # create database 
# MAGIC 
# MAGIC spark.sql("create database if not exists sales")
# MAGIC 
# MAGIC # Create the table.
# MAGIC spark.sql("CREATE TABLE if not exists " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail staging.datecityitem;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table wwi.sales_order;

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

# MAGIC %sql
# MAGIC select count(*) from wwi.warehouse_stockitemtransaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from wwi.sales_customertransaction gro;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count from wwi.sales_customertransaction group by CustomerTransactionID having count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select sl.InvoiceID,sil.invoicelineid, sol.orderlineid,sol.quantity as solquantity, sil.quantity as silquantity from (wwi.sales_invoice sl join wwi.sales_invoiceline sil on sl.invoiceid = sil.invoiceid) join wwi.sales_orderline sol on sol.orderid = sl.orderid and sil.stockitemid = sol.stockitemid where sol.quantity != sil.quantity

# COMMAND ----------

# MAGIC %sql
# MAGIC describe details sales_invoice

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.2015_usa_weather_data_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(Date), min(Date) from wwi.2015_usa_weather_data_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.sales_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct city.stateprovinceid, city.cityname from wwi.sales_customers sc join wwi.sales_order so on sc.customerid = so.customerid join wwi.application_cities city on city.cityid = sc.DeliveryCityID join wwi.application_countries countries on countries.StateProvinceID = city.StateProvinceID

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.sanfrancisco_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.sanf_all_weather limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.sanf_all_weather ;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.*;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.dwdim_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.dwdim_customercategory;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.dwdim_stockitemstockgroup;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dw.dim_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail dw.dim_city;

# COMMAND ----------


display(dw.dim_city.history())

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo ls /mnt

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.sales_invoiceline;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dw.fact_transaction_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table wwi.california_weather_part10;
# MAGIC drop table wwi.california_weather_part11;
# MAGIC drop table wwi.california_weather_part12;

# COMMAND ----------

# MAGIC %md
# MAGIC # Add weather and bridge city and station

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.California_Weather;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.California_Weather limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct station) from wwi.California_Weather where TAVG is not null ;

# COMMAND ----------

# MAGIC %sql
# MAGIC --get the station where tmp is not null
# MAGIC select count(distinct station, name, latitude, longitude) from wwi.California_Weather where TAVG is not null ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct station from wwi.California_Weather where TAVG is not null ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct station, name, cast(latitude as float) as stationlatitude, cast(longitude as float) as stationlongitude  from wwi.California_Weather where TAVG is not null and latitude is not null and longitude is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct station, cast(latitude as float) as stationlatitude, cast(longitude as float) as stationlongitude  from wwi.California_Weather where TAVG is not null and latitude is not null and longitude is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select distinct station, name, cast(latitude as float) as stationlatitude, cast(longitude as float) as stationlongitude  from wwi.California_Weather where TAVG is not null) where station = "USR0000CSRS";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table wwi.city_station;
# MAGIC drop table wwi.station;
# MAGIC create table wwi.station as 
# MAGIC (select distinct station, name, cast(latitude as float) as stationlatitude, cast(longitude as float) as stationlongitude  from wwi.California_Weather where TAVG is not null and latitude is not null and longitude is not null);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail wwi.station

# COMMAND ----------

# MAGIC %sql 
# MAGIC select Location from wwi.application_cities;

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE @source geography = 'POINT(0 51.5)';
# MAGIC DECLARE @target geography = 'POINT(-3 56)';
# MAGIC 
# MAGIC SELECT @source.STDistance(@target);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC select cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",2),'(','') as float) as citylongitude, cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",3),')','') as float)as citylatitude, city.cityid, cityname, StateProvinceID from wwi.application_cities city where StateProvinceID like '5';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table wwi.application_cities;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.application_cities where StateProvinceID like '5' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_stateprovinces

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from wwi.station;

# COMMAND ----------

city_sql = """select cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",2),'(','') as float) as citylongitude, cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",3),')','') as float)as citylatitude, city.cityid, cityname from wwi.application_cities city;"""
city_df = spark.sql(city_sql)
station_sql = """select * from wwi.station;"""
station_df = spark.sql(station_sql)
city_df.crossJoin(station_df).schema
# .withColumn("distance",dist_udf(F.col('citylatitude'), F.col('citylongitude'), F.col('stationlatitude'), F.col('stationlongitude')))
# city_df['station_nearest'] = 


# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

def haversine(lat1, lon1, lat2, lon2):
  lat1, lon1, lat2, lon2 = map(F.radians, [lat1, lon1, lat2, lon2])
  return 2*6378*F.sqrt(pow(F.sin((lat2-lat1)/2),2) + F.cos(lat1)*F.cos(lat2)*pow(F.sin((lon2-lon1)/2),2))

dist_udf=F.udf(haversine, FloatType())

city_sql = """select cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",2),'(','') as float) as citylongitude, cast(replace(regexp_extract(city.Location,"POINT\((.+) (.+)\)",3),')','') as float)as citylatitude, city.cityid, cityname from wwi.application_cities city where StateProvinceID like '5';"""
city_df = spark.sql(city_sql)
station_sql = """select * from wwi.station;"""
station_df = spark.sql(station_sql)

city_station_crossed=city_df.crossJoin(station_df).withColumn("distance",haversine(F.col('citylatitude'),F.col('citylongitude'), F.col('stationlatitude'),F.col('stationlongitude')))
# display(city_station_crossed)

# COMMAND ----------

display(city_station_crossed)

# COMMAND ----------

# https://stackoverflow.com/questions/38687212/spark-dataframe-drop-duplicates-and-keep-first
from pyspark.sql import Window 
import pyspark.sql.functions as F
w = Window.partitionBy('cityid').orderBy('distance')
station_near_city = city_station_crossed.withColumn('rank',F.rank().over(w)).where(F.col('rank') == 1)
#https://mrpowers.medium.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
# from pyspark.sql import Window 
# import pyspark.sql.functions as F

# w = Window.partitionBy('datestr')
# data_df = data_df.withColumn("max", F.max(F.col("col1"))\
#     .over(w))\
#     .where(F.col('max') == F.col('col1'))\
#     .drop("max")
# station_near_city=city_station_crossed.where("distance is not null").orderBy('distance').dropDuplicates(subset = ['cityid','cityname','citylongitude','citylatitude'])
station_near_city.count()#1612
# display(city_station_crossed.groupBy('cityid','cityname','citylongitude','citylatitude').min('distance').select('station'))

# COMMAND ----------

spark.sql('drop table station_near_city')
station_near_city.write.mode("overwrite").saveAsTable("ml.station_near_city")

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(distance) from ml.station_near_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ml.station_near_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from station_near_city;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.California_Weather limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ml.california_cities as(
# MAGIC select * from wwi.application_cities where StateProvinceID like '5') ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ml.california_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ml.city_temp;
# MAGIC create table ml.city_temp as(
# MAGIC select sc.cityid,sc.cityname,date,TAVG from 
# MAGIC ml.california_cities cc join ml.station_near_city sc on cc.cityid = sc.cityid 
# MAGIC join wwi.California_Weather cw on cw.station = sc.station order by cw.date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ml.city_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table ml.sales_by_date_city_item;
# MAGIC create table ml.sales_by_date_city_item as 
# MAGIC (
# MAGIC select cast(so.OrderDate as date) orderdate, ac.CityName cityname, sin.stockitemid stockitemid, sum(sin.extendedprice) as sales, cast(ws.ischillerstock as boolean) ischilled
# MAGIC from wwi.sales_order as so 
# MAGIC join wwi.sales_invoice as si 
# MAGIC on so.OrderID = si.OrderID 
# MAGIC join wwi.sales_invoiceline as sin
# MAGIC on si.InvoiceID = sin.InvoiceID
# MAGIC join wwi.sales_customers as sc 
# MAGIC on si.CustomerID = sc.customerID
# MAGIC join wwi.application_cities as ac
# MAGIC on sc.DeliveryCityID = ac.CityID
# MAGIC join wwi.warehouse_stockitems ws
# MAGIC on ws.stockitemid = sin.stockitemid
# MAGIC Group by orderdate,sc.DeliveryCityID, ac.CityName,sin.stockitemid, ws.ischillerstock
# MAGIC having sc.DeliveryCityID in (select CityID from wwi.application_cities where StateProvinceID like '5')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ml.sales_by_date_city_item; 
# MAGIC --11855

# COMMAND ----------

# MAGIC %sql
# MAGIC --test on other join
# MAGIC select count(*) from ml.sales_by_date_city_item s join ml.station_near_city sc on s.cityname = sc.cityname 
# MAGIC join wwi.California_Weather cw on cw.station = sc.station and s.orderdate=cw.date
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- join reduce rows because some temp is missing
# MAGIC -- select count(s.orderdate,s.cityname,stockitemid, ischilled, TAVG, sales) from ml.sales_by_date_city_item s join ml.city_temp ct on s.orderdate=ct.date and s.cityname=ct.cityname ;
# MAGIC select count(*) from ml.sales_by_date_city_item s left join ml.city_temp ct on s.orderdate=ct.date and s.cityname=ct.cityname ; 
# MAGIC -- when it is inner join:11475

# COMMAND ----------

# MAGIC %sql
# MAGIC (
# MAGIC select s.orderdate,s.cityname,stockitemid, ischilled, cast(TAVG as float), sales from ml.sales_by_date_city_item s left join ml.city_temp ct on s.orderdate=ct.date and s.cityname=ct.cityname);

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table ml.dataset;
# MAGIC create table ml.dataset as
# MAGIC (
# MAGIC select s.orderdate,s.cityname,stockitemid, ischilled, cast(TAVG as float), sales from ml.sales_by_date_city_item s left join ml.city_temp ct on s.orderdate=ct.date and s.cityname=ct.cityname);

# COMMAND ----------

# MAGIC %sql
# MAGIC --add time date
# MAGIC alter table ml.dataset add columns (season_id char(1),weekday char(1));
# MAGIC -- weekday(expr)
# MAGIC UPDATE ml.dataset
# MAGIC set weekday = weekday(orderdate);
# MAGIC 
# MAGIC UPDATE ml.dataset
# MAGIC set season_id = '1'
# MAGIC WHERE
# MAGIC lower(date_format(orderdate, 'MMM')) IN (
# MAGIC 'jun',
# MAGIC 'jul',
# MAGIC 'aug'
# MAGIC );
# MAGIC 
# MAGIC UPDATE ml.dataset
# MAGIC set season_id = '2'
# MAGIC WHERE
# MAGIC lower(date_format(orderdate, 'MMM')) IN (
# MAGIC 'dec',
# MAGIC 'jan',
# MAGIC 'feb'
# MAGIC );
# MAGIC 
# MAGIC UPDATE ml.dataset
# MAGIC set season_id = '3'
# MAGIC WHERE
# MAGIC lower(date_format(orderdate, 'MMM')) IN (
# MAGIC 'mar',
# MAGIC 'apr',
# MAGIC 'may'
# MAGIC );
# MAGIC 
# MAGIC UPDATE ml.dataset
# MAGIC set season_id = '4'
# MAGIC WHERE
# MAGIC lower(date_format(orderdate, 'MMM')) IN (
# MAGIC 'sep',
# MAGIC 'oct',
# MAGIC 'nov'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(weekday) from ml.dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT weekday(DATE'2022-01-17');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ml.dataset ;
# MAGIC --0 is monday, 6 is sunday

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table ml.dataset_test1;
# MAGIC create table ml.dataset_test1 as 
# MAGIC (select 
# MAGIC cityname, 
# MAGIC stockitemid, 
# MAGIC ischilled,
# MAGIC TAVG,
# MAGIC sales,
# MAGIC season_id,
# MAGIC weekday
# MAGIC   from ml.dataset
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ml.dataset_test1

# COMMAND ----------


