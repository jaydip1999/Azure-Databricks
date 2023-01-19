# Databricks notebook source
#Overview of Partitioning Dataframes

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

sample=spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True)

# COMMAND ----------

columns=['id', 'name', 'gender', 'age', 'date', 'country']

# COMMAND ----------

sample=spark.read.schema(schema).csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/sample_data.csv',header=True).toDF(*columns)

# COMMAND ----------

sample.show()

# COMMAND ----------

sample.write

# COMMAND ----------

help(sample.write.partitionBy)

# COMMAND ----------

help(sample.write.json)

# COMMAND ----------

help(sample.write.parquet)

# COMMAND ----------

#Partition Spark Dataframe By Single Column

# COMMAND ----------

#Partitioning data by date column

# COMMAND ----------

from pyspark.sql.functions import date_format,to_date,to_timestamp

# COMMAND ----------

sample=sample.withColumn('date',to_date('date','dd/MM/yyyy'))

# COMMAND ----------

sample.show()

# COMMAND ----------

sample.withColumn('date',date_format('date','yyyyMMdd')).show()

# COMMAND ----------

sample.withColumn('date',date_format('date','yyyyMMdd')).coalesce(1).write.partitionBy('date').parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_date')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_date')

# COMMAND ----------

sample.count()

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_date').dtypes

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_date').show()

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_date').count()

# COMMAND ----------

#Partitioning data by month

# COMMAND ----------

sample.withColumn('month',date_format('date','yyyyMM')).show()

# COMMAND ----------

sample.withColumn('month',date_format('date','yyyyMM')).coalesce(1).write.partitionBy('month').parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month',recurse=True)

# COMMAND ----------

sample.withColumn('month',date_format('date','yyyyMM')).coalesce(1).write.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month',partitionBy='month')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month')

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month').count()

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month').show()

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_month').dtypes

# COMMAND ----------

#Partition Dataframes By Multiple Columns

# COMMAND ----------

#Partitioning of data by year,month and then day.

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy'))\
.withColumn('month',date_format('date','MM')).withColumn('day',date_format('date','dd')).show()

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy')).withColumn('month',date_format('date','MM')).withColumn('day',date_format('date','dd')).coalesce(1).write.partitionBy('year','month','day').parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/mega_partitions') 

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/mega_partitions')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/mega_partitions/year=2015/month=05/day=21')

# COMMAND ----------

spark.read.parquet('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/mega_partitions').count()

# COMMAND ----------

#Setup Dataset for Partition Pruning

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/mega_partitions

# COMMAND ----------

sample.withColumn('year',date_format('date','yyyy')).coalesce(1).write.partitionBy('year').csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year') 

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year').count()

# COMMAND ----------

#Reading Data into Spark Dataframes using Partition Pruning

# COMMAND ----------

sample.count()

# COMMAND ----------

from pyspark.sql.functions import col,year

# COMMAND ----------

sample.filter(year(col('date'))==2015).count()

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year/year=2015').count()

# COMMAND ----------

#partition pruning
spark.read.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year',header=True).filter('year=2015').count()

# COMMAND ----------

df_partition=spark.read.csv('dbfs:/FileStore/shared_uploads/c0851929@mylambton.ca/partition_by_year',header=True)

# COMMAND ----------

df_partition.createOrReplaceTempView('df_partition_view')

# COMMAND ----------

spark.sql('show tables').show()

# COMMAND ----------

spark.sql('''select count(1) from df_partition_view where year=2015''').show()

# COMMAND ----------


