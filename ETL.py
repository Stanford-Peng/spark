# Databricks notebook source
# MAGIC %md
# MAGIC /databricks/python3/bin/databricks --version
# MAGIC export DATABRICKS_TOKEN=dapi274425197d540cf8f15c1a358ef0e32f

# COMMAND ----------

# MAGIC %md
# MAGIC /databricks/python3/bin/databricks configure --token

# COMMAND ----------

# MAGIC %md
# MAGIC databricks secrets list-scopes 

# COMMAND ----------

# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("azure-key-vault", "snowflake-user")
password = dbutils.secrets.get("azure-key-vault", "snowflake-password")
# snowflake connection options
options = {
  "sfUrl": "https://servian.australia-east.azure.snowflakecomputing.com/",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "TEST_DB",
  "sfSchema": "TEAM3",
  "sfWarehouse": "LOAD_WH"
}

# COMMAND ----------

spark.range(5).write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "TEST_DB") \
  .save()

# COMMAND ----------


