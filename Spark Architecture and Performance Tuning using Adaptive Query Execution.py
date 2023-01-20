# Databricks notebook source
#Reviewing Properties of Spark Adaptive Execution

# COMMAND ----------

spark

# COMMAND ----------

type(spark)

# COMMAND ----------

spark.conf

# COMMAND ----------

spark.conf.get('spark.master')

# COMMAND ----------

spark.conf.set('spark.master','local')

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

 spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

#Disabling Adaptive Execution and Running Spark App

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

 spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType
schema=StructType([
    StructField('ID',IntegerType()),
    StructField('NAME',StringType()),
    StructField('GENDER',StringType()),
    StructField('AGE',IntegerType()),
    StructField('DATE',StringType()),
    StructField('COUNTRY',StringType()),
])


# COMMAND ----------

columns=['id', 'name', 'gender', 'age', 'date', 'country']


# COMMAND ----------

sample=spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True).toDF(*columns)

# COMMAND ----------


spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True).toDF(*columns).rdd.getNumPartitions()

# COMMAND ----------

spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True).toDF(*columns).printSchema()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)

# COMMAND ----------

spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True).toDF(*columns).show()

# COMMAND ----------

from pyspark.sql.functions import date_format,to_date,to_timestamp

# COMMAND ----------

sample=sample.withColumn('date',to_date('date','dd/MM/yyyy'))

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy'))\
.withColumn('month',date_format('date','MM')).withColumn('day',date_format('date','dd')).show()

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy')).withColumn('month',date_format('date','MM')).withColumn('day',date_format('date','dd')).groupBy('year','month','day').count().write.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups') 

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups')

# COMMAND ----------

df=spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups') 

# COMMAND ----------

len(df.inputFiles())

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

#Run Spark App with Adaptive Execution

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups',recurse=True)

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',True)

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy')).withColumn('month',date_format('date','MM')).withColumn('day',date_format('date','dd')).groupBy('year','month','day').count().write.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups') 

# COMMAND ----------

df=spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups') 

# COMMAND ----------

len(df.inputFiles())

# COMMAND ----------

df.inputFiles() 

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/groups')

# COMMAND ----------


