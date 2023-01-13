# Databricks notebook source
#Create Single Column Spark Dataframe using List

# COMMAND ----------

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

users_list=[(1,'Jaydip'),(2,'Bhavik'),(3,'Vishal'),(4,'Dhaval')]
df=spark.createDataFrame(users_list,'user_id int,user_first_name string')


# COMMAND ----------

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

from pyspark.sql import Row


# COMMAND ----------

help(Row)

# COMMAND ----------

r1=Row("Jaydip", 11)

# COMMAND ----------

r1

# COMMAND ----------

r2=Row(name="Jaydip",age=11)


# COMMAND ----------

r2

# COMMAND ----------

r2.name

# COMMAND ----------


r2['name']

# COMMAND ----------

users_list=[[1,'Jaydip'],[2,'Bhavik'],[3,'Dhaval'],[4,'Vishal']]

# COMMAND ----------

type(users_list)

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list,'user_id int,user_first_name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users_rows=[Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows,'user_id int,user_first_name string')

# COMMAND ----------

def dummy(*args):
    print(args)
    print(len(args))

# COMMAND ----------

dummy(1)

# COMMAND ----------

dummy(1,'hello')

# COMMAND ----------

user_details=[1,'jaydip']

# COMMAND ----------

dummy(user_details)

# COMMAND ----------

dummy(*user_details)

# COMMAND ----------

users_list=[(1,'Jaydip'),(2,'Bhavik'),(3,'Dhaval'),(4,'Vishal')]

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list,'user_id int,user_first_name string')

# COMMAND ----------

from pyspark.sql import Row
users_rows=[Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows,'user_id int,user_first_name string')

# COMMAND ----------

users_list=[{'user_id':1,'user_first_name':'Jaydip'},
           {'user_id':2,'user_first_name':'Bhavik'},
           {'user_id':3,'user_first_name':'Dhaval'},
           {'user_id':4,'user_first_name':'Vishal'}]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

from pyspark.sql import Row
users_rows=[Row(**user) for user in users_list]
