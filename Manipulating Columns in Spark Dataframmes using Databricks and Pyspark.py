# Databricks notebook source
#Predefined Functions using Spark Dataframe APIs 

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

#read the data
sample = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv")

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

sample.show()

# COMMAND ----------

sample.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

sample=sample.select(col('ID').cast('int'),col('NAME'),col('GENDER'),col('AGE').cast('int'),col('DATE '),col('COUNTRY'))

# COMMAND ----------

sample.printSchema()

# COMMAND ----------

help(date_format)

# COMMAND ----------

sample.select('*',date_format('DATE ','ddmmyyyy').alias('ORDER_MONTH')).show()

# COMMAND ----------

#filter function

# COMMAND ----------

#function as a part of filter/where
sample.filter(col('DATE ')=='21/05/2015').show()

# COMMAND ----------

#funciton as a part of groupBy
sample.groupBy(col('DATE ')).count().show()

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

help(F.date_format)

# COMMAND ----------

#creating dummy df

# COMMAND ----------

l1=[('a',)]

# COMMAND ----------


df=spark.createDataFrame(l1,'dummy string')

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date
df.select(current_date()).show()

# COMMAND ----------

df.select(current_date().alias('today_date')).show()

# COMMAND ----------

users_list=[(1,'Jaydip'),(2,'Bhavik'),(3,'Vishal'),(4,'Dhaval')]
df=spark.createDataFrame(users_list,'user_id int,name string')

# COMMAND ----------

df.show()

# COMMAND ----------

#Categories of Functions to Manipulate Columns in Spark Dataframes

# COMMAND ----------

from pyspark.sql.functions import lower

# COMMAND ----------

help(lower)

# COMMAND ----------

df.withColumn('lower_name',lower('name')).show()

# COMMAND ----------

#special functions- col and lit

# COMMAND ----------

sample.show()

# COMMAND ----------

sample.select('NAME','GENDER').show()

# COMMAND ----------

sample.groupby('GENDER').count().show()

# COMMAND ----------

sample.orderBy('AGE').show()

# COMMAND ----------

help(col)

# COMMAND ----------

type(col('AGE'))

# COMMAND ----------

sample.select(col('NAME'),col('GENDER')).show()

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

help(upper)

# COMMAND ----------

type(upper('NAME'))

# COMMAND ----------

sample.groupby(upper('GENDER')).count().show()

# COMMAND ----------

from pyspark.sql import Column
help(Column)

# COMMAND ----------

from pyspark.sql.column import Column
help(Column)

# COMMAND ----------

sample.orderBy('AGE'.desc()).show()

# COMMAND ----------

sample.orderBy( col('AGE').desc()).show()

# COMMAND ----------

sample.orderBy(upper('GENDER')).show()

# COMMAND ----------

from pyspark.sql.functions import concat,lit

# COMMAND ----------

sample.select(concat(col('NAME'),",",col('AGE'))).show()

# COMMAND ----------

sample.select(concat(col('NAME'),lit(","),col('AGE'))).show()

# COMMAND ----------

sample.withColumn('age','AGE'*lit(0.2)).show()

# COMMAND ----------

sample.withColumn('age',col('AGE')*lit(0.2)).show()

# COMMAND ----------

#Common String Manipulation Functions 
from pyspark.sql.functions import concat_ws,initcap,length


# COMMAND ----------

sample.select(concat(col('NAME'),lit(","),col('AGE'))).show()

# COMMAND ----------

sample.select(concat_ws(', ',col('NAME'),col('AGE'))).show()

# COMMAND ----------

sample.select('ID','NAME').\
withColumn('NAME_upper',upper('NAME')).\
withColumn('NAME_lower',lower('NAME')).\
withColumn('NAME_intitcap',initcap('NAME')).\
withColumn('NAME_length',length('NAME')).\
show()


# COMMAND ----------

#Extraction of strings using substring

# COMMAND ----------

from pyspark.sql.functions import substring

# COMMAND ----------

help(substring)

# COMMAND ----------

df.select(substring(lit('hello world'),7,5)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(substring(lit('hello world'),-5,5)).show()

# COMMAND ----------

#extraction of strings using split 

# COMMAND ----------

from pyspark.sql.functions import split,explode

# COMMAND ----------

df.select(split(lit('hello world, how are you'),' ')).show(truncate=False)

# COMMAND ----------

df.select(split(lit('hello world, how are you'),' ')[2]).show(truncate=False)

# COMMAND ----------

df.select(explode(split(lit('hello world, how are you'),' ')).alias('word')).show(truncate=False)

# COMMAND ----------

#padding character around strings

# COMMAND ----------

l1=[('a',)]
df=spark.createDataFrame(l1,'dummy string')

# COMMAND ----------

from pyspark.sql.functions import lit,lpad,rpad,concat

# COMMAND ----------

help(lpad)

# COMMAND ----------

df.select(lpad(lit('hello'),10,'-').alias('dummy')).show()

# COMMAND ----------

sample.show()

# COMMAND ----------

new_sample=sample.select(concat(lpad('ID',5,'0'),rpad('NAME',10,'-'),rpad('GENDER',10,'x'),lpad('AGE',5,'0'),rpad('COUNTRY',10,'-'),rpad('DATE ',10,'d')).alias('details'))

# COMMAND ----------

new_sample.show(truncate=False)

# COMMAND ----------


