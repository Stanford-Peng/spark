# Databricks notebook source
access_key = dbutils.secrets.get("azure-key-vault", "storage-account-access-key")
spark.conf.set(
    "fs.azure.account.key.datalaketeam3.dfs.core.windows.net",
   access_key)

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f


# COMMAND ----------

# MAGIC %md
# MAGIC # Sales order ( 3 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC --ERROR 1
# MAGIC SELECT OrderID, count(*)
# MAGIC FROM wwi.sales_order
# MAGIC group by OrderID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.sales_order
# MAGIC WHERE OrderID = 70170

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE wwi.sales_order
# MAGIC SET IsUndersupplyBackordered = UPPER(IsUndersupplyBackordered)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.sales_order
# MAGIC WHERE OrderID = 70170

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW COLUMNS IN default.sales_order;
# MAGIC SELECT CustomerPurchaseOrderNumber, count(*) as dups
# MAGIC FROM wwi.sales_order
# MAGIC group by CustomerPurchaseOrderNumber
# MAGIC having count(*) > 1

# COMMAND ----------

#ERROR 2
#remove dulicate rows except OrderID

df_original = spark.sql("SELECT * FROM WWI.sales_order")
#df_original.groupBy([col for col in df_original.columns if col!='OrderID']).agg(f.count("*").alias("count")).where(f.col("count") >1).show(truncate=False)
df_cleaned = df_original.dropDuplicates(subset=[col for col in df_original.columns if col!='OrderID'])
df_cleaned.groupBy([col for col in df_original.columns if col!='OrderID']).agg(f.count("*").alias("count")).where(f.col("count") >1).show(truncate=False)
df_cleaned.write.mode("overwrite").saveAsTable("WWI.sales_order")

# COMMAND ----------

df_original = spark.sql("SELECT * FROM WWI.sales_order")
df_original.groupBy([col for col in df_original.columns if col!='OrderID']).agg(f.count("*").alias("count")).where(f.col("count") >1).show(truncate=False)


# COMMAND ----------

#ERROR 3
df_original = spark.sql("SELECT CustomerPurchaseOrderNumber FROM wwi.sales_order")
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

df_original = spark.sql("SELECT * FROM wwi.sales_order")

df_error = df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5)

pddf = df_error.toPandas()
pddf1 = pddf.loc[:,"OrderID":"BackorderOrderID":1]
pddf2 = pddf.loc[:,"OrderDate"::1].shift(periods=-1, axis="columns")


spark.conf.set("spark.sql.execution.arrow.enabled", "true")
df_concat = pd.concat([pddf1, pddf2], axis=1)

spdf_concat = pandas_to_spark(df_concat)

spdf_concat.write.mode("append").saveAsTable("wwi.sales_order")
#only action fire the lazy loading spark
spark.sql("DELETE FROM wwi.sales_order where length(CustomerPurchaseOrderNumber) > 5")


# COMMAND ----------

df_original = spark.sql("SELECT CustomerPurchaseOrderNumber FROM wwi.sales_order")
df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.sales_order
# MAGIC where OrderDate IS NULL
# MAGIC --SHOW COLUMNS IN sales_orderlines_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 10
# MAGIC SELECT *
# MAGIC FROM wwi.sales_order
# MAGIC WHERE CustomerID NOT IN 
# MAGIC (select CustomerID from wwi.sales_customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 11
# MAGIC SELECT *
# MAGIC FROM wwi.sales_order
# MAGIC where OrderDate > ExpectedDeliveryDate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct PickingCompletedWhen
# MAGIC FROM wwi.sales_order

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Customers (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wwi.sales_customers;
# MAGIC --SHOW COLUMNS IN wwi.sales_customers;

# COMMAND ----------

df_original = spark.sql("SELECT * FROM WWI.sales_customers")
df_original.groupBy([col for col in df_original.columns if col!='CustomerID']).agg(f.count("*").alias("count")).where(f.col("count") >1).show(truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 1
# MAGIC --SHOW COLUMNS IN default.sales_order;
# MAGIC SELECT CustomerID, count(*) as dups
# MAGIC FROM wwi.sales_customers
# MAGIC group by CustomerID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 2
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE CustomerID IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 3
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE CustomerCategoryID NOT IN
# MAGIC (select CustomerCategoryID from wwi.sales_customercategories)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 4
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE BuyingGroupID NOT IN
# MAGIC (select BuyingGroupID from wwi.sales_buyinggroups)

# COMMAND ----------

# MAGIC  %sql
# MAGIC --CHECK 5
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE PrimaryContactPersonID NOT IN --AlternateContactPersonID also
# MAGIC (select PersonID from wwi.application_people)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 6
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE DeliveryMethodID NOT IN
# MAGIC (select DeliveryMethodID from wwi.application_paymentmethods)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CHECK 7
# MAGIC SELECT *
# MAGIC FROM wwi.sales_customers
# MAGIC WHERE DeliveryCityID NOT IN --PostalCityID also
# MAGIC (select CityID from wwi.application_cities)

# COMMAND ----------



# COMMAND ----------

# CHECK 8
df_original = spark.sql("SELECT AccountOpenedDate FROM wwi.sales_customers")

df = df_original.withColumn("first_n_char", df_original.AccountOpenedDate.substr(1,10))
df.select('first_n_char').distinct().show(df.count(),False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Invoicelines (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT InvoiceID, StockItemID, count(*)
# MAGIC FROM wwi.sales_invoiceline
# MAGIC group by InvoiceID, StockItemID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct LastEditedBy
# MAGIC FROM wwi.sales_invoiceline

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Colors (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  ColorName, count(*) --distinct ColorID
# MAGIC from wwi.warehouse_colours
# MAGIC group by  ColorName
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct LastEditedBy
# MAGIC from wwi.warehouse_colours

# COMMAND ----------


df_original = spark.sql("SELECT ValidTo FROM wwi.warehouse_colours")
df_original.filter(f.length(f.col("ValidTo")) >27).show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Stockitemsgroups (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  
# MAGIC from wwi.warehouse_stockitemstockgroups

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM wwi.warehouse_stockitemstockgroups
# MAGIC WHERE StockItemID IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select StockItemID,StockGroupID, count(*)
# MAGIC from wwi.warehouse_stockitemstockgroups
# MAGIC group by StockItemID,StockGroupID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT distinct LastEditedWhen
# MAGIC FROM wwi.warehouse_stockitemstockgroups
# MAGIC WHERE LENGTH(LastEditedWhen) < 27

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct LastEditedBy
# MAGIC FROM wwi.warehouse_stockitemstockgroups

# COMMAND ----------

# MAGIC %sql
# MAGIC select StockGroupID, count(*)
# MAGIC from wwi.warehouse_stockitemstockgroups
# MAGIC group by StockGroupID
# MAGIC --having count(*) > 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Coldroomtemp (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_coldroomtemp

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Vehicletemp (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_vehicletemperature

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_vehicletemperature
# MAGIC WHERE RecordedWhen IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_vehicletemperature
# MAGIC WHERE Temperature IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VehicleRegistration,ChillerSensorNumber,RecordedWhen, count(*)
# MAGIC FROM wwi.warehouse_vehicletemperature
# MAGIC GROUP BY VehicleRegistration,ChillerSensorNumber,RecordedWhen
# MAGIC having count(*) > 1

# COMMAND ----------

#df_original = spark.sql("SELECT RecordedWhen FROM wwi.sales_order")
#df_original.filter(f.length(f.col("CustomerPurchaseOrderNumber")) >5).show()
# CHECK 8
df_original = spark.sql("SELECT RecordedWhen FROM wwi.warehouse_vehicletemperature")
df_original.select('RecordedWhen').distinct().filter(f.length(f.col("RecordedWhen")) !=27).show()
#df = df_original.withColumn("first_n_char", df_original.RecordedWhen.substr(1,10))
#df.select('first_n_char').distinct().filter(f.length(f.col("first_n_char")) <10).show(df.count(),False)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT distinct IsCompressed
# MAGIC FROM wwi.warehouse_vehicletemperature

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_vehicletemperature
# MAGIC WHERE VehicleRegistration not like 'WWI%'

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC SELECT distinct VehicleRegistration
# MAGIC FROM wwi.warehouse_vehicletemperature

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_vehicletemperature
# MAGIC WHERE Temperature LIKE '%[a-zA-Z]%'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Stockitemtransaction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_stockitemtransaction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT StockItemID, InvoiceID,count(*)
# MAGIC FROM wwi.warehouse_stockitemtransaction
# MAGIC WHERE  InvoiceID is not null
# MAGIC GROUP BY StockItemID, InvoiceID 
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.warehouse_stockitemtransaction
# MAGIC WHERE StockItemID NOT IN (
# MAGIC SELECT distinct StockItemID
# MAGIC FROM wwi.warehouse_stockitems)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct TransactionTypeID
# MAGIC FROM wwi.warehouse_stockitemtransaction

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Suppliers (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM wwi.purchase_suppliers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  SupplierName, count(*)
# MAGIC FROM wwi.purchase_suppliers
# MAGIC GROUP BY SupplierName
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  SupplierName
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE SupplierName like '%[0-9]%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE SupplierCategoryID NOT IN (
# MAGIC SELECT distinct SupplierCategoryID
# MAGIC FROM wwi.purchase_suppliercategories)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE AlternateContactPersonID --PrimaryContactPersonID also
# MAGIC NOT IN (
# MAGIC SELECT distinct PersonID
# MAGIC FROM wwi.application_people)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE DeliveryMethodID NOT IN (
# MAGIC SELECT distinct DeliveryMethodID
# MAGIC FROM wwi.application_paymentmethods)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE DeliveryCityID NOT IN (
# MAGIC SELECT distinct CityID
# MAGIC FROM wwi.application_cities)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE PostalCityID NOT IN (
# MAGIC SELECT distinct CityID
# MAGIC FROM wwi.application_cities)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE BankAccountName LIKE '%[0-9]%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE BankAccountNumber LIKE '%[a-z]%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE PhoneNumber LIKE '%[a-z]%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE WebsiteURL NOT LIKE '%.com'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE WebsiteURL NOT LIKE 'http://www.%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DeliveryLocation, count(*)
# MAGIC FROM wwi.purchase_suppliers
# MAGIC GROUP BY DeliveryLocation
# MAGIC HAVING count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC FROM wwi.purchase_suppliers
# MAGIC WHERE DeliveryLocation like '%(-122.4194155 37.7749295)'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.purchase_purchaseorders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM (
# MAGIC SELECT SupplierID,OrderDate,DeliveryMethodID,ContactPersonID,ExpectedDeliveryDate,SupplierReference,IsOrderFinalized,Comments,InternalComments,LastEditedBy,LastEditedWhen, count(*)
# MAGIC FROM wwi.purchase_purchaseorders
# MAGIC GROUP BY SupplierID,OrderDate,DeliveryMethodID,ContactPersonID,ExpectedDeliveryDate,SupplierReference,IsOrderFinalized,Comments,InternalComments,LastEditedBy,LastEditedWhen
# MAGIC having count(*) > 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT PurchaseOrderID, count(*)
# MAGIC FROM wwi.purchase_purchaseorders
# MAGIC GROUP BY PurchaseOrderID
# MAGIC having count(*) > 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Orderslines (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.purchase_purchaseorderlines

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PurchaseOrderID,StockItemID,count(*)
# MAGIC FROM wwi.purchase_purchaseorderlines
# MAGIC GROUP BY PurchaseOrderID,StockItemID
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM wwi.purchase_purchaseorderlines
# MAGIC WHERE PurchaseOrderID NOT IN (
# MAGIC SELECT distinct PurchaseOrderID 
# MAGIC FROM wwi.purchase_purchaseorders)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Supplier Categories (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM wwi.purchase_suppliercategories

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Supplier Transactions (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM wwi.purchase_suppliertransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SupplierID,TransactionTypeID,PurchaseOrderID,count(*)
# MAGIC FROM wwi.purchase_suppliertransactions
# MAGIC WHERE PurchaseOrderID IS NOT NULL
# MAGIC GROUP BY SupplierID,TransactionTypeID,PurchaseOrderID
# MAGIC having count(*) > 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Application Cities (1 Error)

# COMMAND ----------

# MAGIC %sql
# MAGIC --show data
# MAGIC select * 
# MAGIC from wwi.application_cities 
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select Location, count(*) as count 
# MAGIC from wwi.application_cities 
# MAGIC group by Location 
# MAGIC having count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_cities  
# MAGIC where location like 'POINT (-66.1287777 18.2185674)'

# COMMAND ----------

# MAGIC %sql
# MAGIC --Error fix 1
# MAGIC UPDATE wwi.application_cities 
# MAGIC SET Location = 'POINT (-66.123700 18.222900)' 
# MAGIC WHERE CityID = 33142;

# COMMAND ----------

# MAGIC %sql
# MAGIC select Location, count(*) as count 
# MAGIC from wwi.application_cities 
# MAGIC group by Location 
# MAGIC having count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 1
# MAGIC SELECT CityID, StateProvinceID 
# MAGIC FROM wwi.application_cities 
# MAGIC Group BY CityID, StateProvinceID 
# MAGIC HAVING count(*)>1 --both should be unique ,,,,city name can be same but cannot be in the same province

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 2
# MAGIC select  count(*) 
# MAGIC FROM wwi.application_cities 
# MAGIC where (LatestRecordedPopulation is null) or (LatestRecordedPopulation = 0) 
# MAGIC --missing values in LatestRecordedPopulation. we can fill it or do a null where unknown

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 3
# MAGIC select count(*) 
# MAGIC FROM wwi.application_cities 
# MAGIC where LatestRecordedPopulation < 0 --checking negative values

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECK WITH GULBAZ

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 4
# MAGIC 
# MAGIC select count(*) 
# MAGIC FROM wwi.application_cities as a 
# MAGIC join wwi.sales_customers as s 
# MAGIC on   a.CityID = s.PostalCityID 
# MAGIC where DeliveryCityID is not null; 

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 5
# MAGIC select count(*) 
# MAGIC FROM wwi.application_cities 
# MAGIC where CityID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 6
# MAGIC --check if the city is not repeated in the same state
# MAGIC SELECT count(*), CityName, StateProvinceID 
# MAGIC FROM wwi.application_cities 
# MAGIC Group By CityName, StateProvinceID 
# MAGIC having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from wwi.application_cities 
# MAGIC where StateProvinceID not in (select StateProvinceID from wwi.application_stateprovinces)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Application Countries (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_countries limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 1
# MAGIC --check if country ids are unique
# MAGIC select count (distinct CountryID) 
# MAGIC from wwi.application_countries

# COMMAND ----------

# MAGIC %sql
# MAGIC --test3
# MAGIC select count(*) 
# MAGIC from wwi.application_countries 
# MAGIC where FormalName is null

# COMMAND ----------

#test 4
df = spark.sql("select * from wwi.application_countries")
df.filter(f.length(f.col("IsoAlpha3Code")) > 3).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Application State Province (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC --test 5 
# MAGIC select count(*) 
# MAGIC from wwi.application_stateprovinces 
# MAGIC where CountryID not in (select CountryID from wwi.application_countries)

# COMMAND ----------

# MAGIC %sql
# MAGIC --manual 53 rows
# MAGIC select * 
# MAGIC from wwi.application_stateprovinces 
# MAGIC where StateProvinceID like '40'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Application People (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  count(*) ,PhoneNumber
# MAGIC FROM wwi.application_people 
# MAGIC --WHERE length(PhoneNumber) > 14
# MAGIC group by PhoneNumber
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_people
# MAGIC where PhoneNumber like '(787) 555-0100'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --needs a lot of cleaning Discuss with team
# MAGIC select FullName, count(*) --distinct Photo
# MAGIC from wwi.application_people 
# MAGIC --where EmailAddress IS NULL
# MAGIC group by FullName 
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from wwi.application_people 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_people 
# MAGIC where LogonName not like '%wideworldimporters.com' AND LogonName not like 'NO LOGON' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_people 
# MAGIC where LogonName not in (select LogonName from wwi.application_people where LogonName like '%tailspintoys.com' or LogonName like '%wideworldimporters.com' )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_people 
# MAGIC where (IsPermittedToLogon not like 'True' and LogonName like "%.com")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_people 
# MAGIC where  EmailAddress not like "%.com"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from wwi.application_people 
# MAGIC where  EmailAddress not like "%@%"

# COMMAND ----------

# MAGIC %md
# MAGIC # Application Deliverymethods and Application Paymentmethods (1 Error)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_deliverymethods

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wwi.application_paymentmethods

# COMMAND ----------

# MAGIC %sql
# MAGIC --error fix 2
# MAGIC Alter table wwi.application_deliverymethods rename to wwi.application_paymentmethods_new;
# MAGIC Alter table wwi.application_paymentmethods rename to wwi.application_deliverymethods_new;
# MAGIC Alter table wwi.application_paymentmethods_new rename to wwi.application_paymentmethods;
# MAGIC Alter table wwi.application_deliverymethods_new rename to wwi.application_deliverymethods;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_deliverymethods

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wwi.application_paymentmethods

# COMMAND ----------

# MAGIC %md
# MAGIC # Application Transaction Types (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_transactiontypes

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Application System Parameters (0 Errors)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wwi.application_systemparameters

# COMMAND ----------



# COMMAND ----------


