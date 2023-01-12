# Databricks notebook source
users_list=[(1,'Jaydip'),(2,'Bhavik'),(3,'Vishal'),(4,'Dhaval')]
df=spark.createDataFrame(users_list,'user_id int,user_first_name string')

# COMMAND ----------

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------



# COMMAND ----------

type(df.collect())

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


