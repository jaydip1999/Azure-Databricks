# Databricks notebook source
ages_list=[21,23,18,41,32]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(ages_list)

# COMMAND ----------

spark.createDataFrame(ages_list,'int')

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

spark.createDataFrame(ages_list,IntegerType())

# COMMAND ----------

names_list=['Jaydip','Bhavik','Dhaval']

# COMMAND ----------

spark.createDataFrame(ages_list,'string')

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

spark.createDataFrame(ages_list,StringType())

# COMMAND ----------

ages_list=[(21,),(23,),(41,),(32,)]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

type(ages_list[2])

# COMMAND ----------

spark.createDataFrame(ages_list,'age int')

# COMMAND ----------

users_list=[(1,'Jaydip'),(2,'Bhavik'),(3,'Dhaval'),(4,'Meet')]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list,'user_id int, user_first_name string')

# COMMAND ----------


