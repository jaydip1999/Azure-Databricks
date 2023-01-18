# Databricks notebook source
#Validating Datasets for Reading from Files using Spark APIs 

# COMMAND ----------

# MAGIC %fs ls /public/retail_db

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

schema='''order_id int, order_date timestamp, order_customer_id int, order_status string'''

# COMMAND ----------

orders-spark.read.schema(schema).csv('/public/retail_db/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

#Converting JSON files to parquet files

# COMMAND ----------

# MAGIC %fs ls /public/read_db_json 

# COMMAND ----------

# MAGIC %fs ls /public/read_db_json/orders

# COMMAND ----------

orders=spark.read.json('/public/retail_db_json/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

import getpass
username=getpass.getuser()

# COMMAND ----------

output_dir= f'/user/{username}/retail_db_parquet'
input_dir= '/public/read_db_json'

# COMMAND ----------

 dbutils.fs.ls(input_dir)

# COMMAND ----------

for file_details in  dbutils.fs.ls(input_dir):
    if not('.git' in file_details.path or file_details.path.endswith('sql')):
        print(f'converting data in {file_details.path} folder from json to parquet')
        dataset_dir=file_details.path.split('/')[-2]
        df=spark.read.json(file_details.path)
        df.coalesce(1).write.parquet(f'{output_dir}/{dataset_dir}',mode='overwrite'')
        

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders=spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.show()

# COMMAND ----------

#Converting Comma Seperated FIles to Pipe Seperated Files using Spark

# COMMAND ----------

import getpass
username=getpass.getuser()
 output_dir= f'/user/{username}/retail_db_pipe'
input_dir= '/public/read_db'

# COMMAND ----------

 dbutils.fs.ls(input_dir)

# COMMAND ----------

for file_details in  dbutils.fs.ls(input_dir):
    if '.git' not in file_details.path or 'sql' not in file_details.path:
        print(f'converting data in {file_details.path} folder from comma seperated to pipe seperated')
        folder_name=file_details.path.split('/')[-2]
        df=spark.read.json(file_details.path)
        df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/{folder_name}',sep='|')
        

# COMMAND ----------

print(schema)

# COMMAND ----------

orders=spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders=spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders',sep='|')

# COMMAND ----------

orders.show()

# COMMAND ----------

#Overview of Reading Data Files into Spark Dataframes

# COMMAND ----------

type(spark.read)

# COMMAND ----------

spark

# COMMAND ----------

type(spark)

# COMMAND ----------

type(spark.read)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


