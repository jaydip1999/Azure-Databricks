# Databricks notebook source
#Registration of UDFs.

# COMMAND ----------

help(spark.udf.register)

# COMMAND ----------

#Using Spark UDFs as part of Dataframe APIs

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

from pyspark.sql.functions import date_format,to_date,to_timestamp

# COMMAND ----------

sample=sample.withColumn('date',to_date('date','dd/MM/yyyy'))

# COMMAND ----------

sample.dtypes

# COMMAND ----------

sample=sample.withColumn('date',sample['date'].cast('string'))

# COMMAND ----------

sample.dtypes

# COMMAND ----------

dc=spark.udf.register('new_date',lambda d: int(d[:10].replace('-','')))

# COMMAND ----------

dc

# COMMAND ----------

sample.select(dc('date').alias('date')).show()

# COMMAND ----------

sample.filter(dc('date')==20160521).show()

# COMMAND ----------

sample.groupBy(dc('date')).count().show()

# COMMAND ----------

#Using Spark UDFs as part of Spark SQL

# COMMAND ----------


