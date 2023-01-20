# Databricks notebook source
#Validation of Datasets for Writing into Files from Dataframes using Spark APIs.

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv

# COMMAND ----------

#Overview Dataframes into Files

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users=[
    {
        'user_id':1,
        'user_first_name':'Jaydip',
        'user_last_name':'Dobariya',
        'user_email':'dobariyajaydip@gmail.com'
    },
    {
        'user_id':2,
        'user_first_name':'Vishal',
        'user_last_name':'Barvaliya',
        'user_email':'barvaliyavishal@gmail.com'
    },
    {
        'user_id':3,
        'user_first_name':'Bhavik',
        'user_last_name':'Gajera',
        'user_email':'gajerabhavik@gmail.com'
    },
    {
        'user_id':4,
        'user_first_name':'Dhaval',
        'user_last_name':'Kathiriya',
        'user_email':'kathiriyadhaval@gmail.com'
    },
    {
        'user_id':5,
        'user_first_name':'Meet',
        'user_last_name':'Ambaliya',
        'user_email':'ambaliyameet@gmail.com'
    },
    {
        'user_id':6,
        'user_first_name':'Shyam',
        'user_last_name':'Kaveri',
        'user_email':'kaverishyam@gmail.com'
    },
    {
        'user_id':7,
        'user_first_name':'Krutik',
        'user_last_name':'Shiroya',
        'user_email':'shiroyakrutik@gmail.com'
    },
    {
        'user_id':8,
        'user_first_name':'Jenish',
        'user_last_name':'Thummar',
        'user_email':'thummarjenish@gmail.com'
    },
    {
        'user_id':9,
        'user_first_name':'Sanket',
        'user_last_name':'Bhimani',
        'user_email':'bhimanisanket@gmail.com'
    },
    {
        'user_id':10,
        'user_first_name':'Jay',
        'user_last_name':'Chothani',
        'user_email':'jaychothani@gmail.com'
    }
]
users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

type(users_df.write)

# COMMAND ----------

users_df.write.json('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

users_df.write.format('json').save('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite')

# COMMAND ----------

#Writing Spark Dataframes into CSV files

# COMMAND ----------

users_df.write.format('csv').save('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

#using spark.read.text we can read the raw data as single column df
#while writing data into csv file comma is considered as seperator/delimeter by default.
spark.read.text('/FileStore/shared_uploads/c0851929@mylambton.ca/users').show(truncate=False)

# COMMAND ----------

#Specifying Header

# COMMAND ----------

users_df.coalesce(1).write.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite',header=True)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

spark.read.text('/FileStore/shared_uploads/c0851929@mylambton.ca/users').show(truncate=False)

# COMMAND ----------

spark.read.csv('/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True).show(truncate=False)

# COMMAND ----------

#Using Compression while writing Spark Dataframe into CSV Files.

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

help(users_df.write.csv)

# COMMAND ----------

users_df.coalesce(1).write.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite',compression='gzip',header=True)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

spark.read.csv('/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True).show(truncate=False)

# COMMAND ----------

users_df.coalesce(1).write.format('csv').save('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',mode='overwrite',compression='snappy',header=True)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

spark.read.csv('/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True).show(truncate=False)

# COMMAND ----------

#Specifying Delimiter 

# COMMAND ----------

users_df.coalesce(1).write.mode('overwrite').csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True,sep='|')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/c0851929@mylambton.ca/users')

# COMMAND ----------

spark.read.csv('/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True).show(truncate=False)

# COMMAND ----------

spark.read.csv('/FileStore/shared_uploads/c0851929@mylambton.ca/users',header=True,sep='|').show(truncate=False)

# COMMAND ----------


