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

#Trimming

# COMMAND ----------

l1=[('   a.  ',)]
df=spark.createDataFrame(l1,'dummy string')

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import ltrim,rtrim,trim

# COMMAND ----------

help(trim)

# COMMAND ----------

help(ltrim)

# COMMAND ----------

help(rtrim)

# COMMAND ----------

df.withColumn('ltrim',ltrim(col('dummy'))).withColumn('rtrim',rtrim(col('dummy'))).withColumn('trim',trim(col('dummy'))).show()

# COMMAND ----------

spark.sql('describe function rtrim').show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

df.withColumn('ltrim',expr("ltrim(dummy)")).\
withColumn('rtrim',expr("rtrim('.',rtrim(dummy))")).withColumn('trim',trim(col('dummy'))).show()

# COMMAND ----------

spark.sql('describe function trim').show(truncate=False)

# COMMAND ----------

df.withColumn('ltrim',expr("trim(lEADING ' ' FROM dummy)")).\
withColumn('rtrim',expr("trim(TRAILING '.' FROM rtrim(dummy))")).withColumn('trim',expr("trim(BOTH ' ' FROM dummy )")).show()

# COMMAND ----------

#Date and Time Manipulation Functions

# COMMAND ----------

from pyspark.sql.functions import current_date,current_timestamp,to_date,to_timestamp

# COMMAND ----------

help(current_date)

# COMMAND ----------

df.select(current_date()).show()#yyyy-MM-dd

# COMMAND ----------

df.select(current_timestamp()).show(truncate=False)#yyyy-MM-dd HH:mm:ss.SSS

# COMMAND ----------

help(to_date)

# COMMAND ----------

df.select(to_date(lit('20220229'),'yyyymmdd').alias('to_date')).show()#converting non-standard date format to standard date format

# COMMAND ----------

df.select(to_timestamp(lit('20220229 1725'),'yyyyMMdd HHmm').alias('to_timestamp')).show() #converting non-standard datetime format to standard datetime format

# COMMAND ----------

#Date and Time Arithmetic using Spark Dataframes

# COMMAND ----------

datetimes=[('2014-02-28','2014-02-28 10:00:00.123'),('2016-02-29','2016-02-29 08:08:08.999'),('2017-10-31','2017-10-31 11:59:59.123')]

# COMMAND ----------

dtdf=spark.createDataFrame(datetimes,schema='date string ,time string')

# COMMAND ----------

dtdf.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import date_add,date_sub

# COMMAND ----------


help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

#add 10 days and subtract 10 days to both date and time values

# COMMAND ----------

dtdf.withColumn('date_add_date',date_add('date',10)).withColumn('date_add_time',date_add('time',10)).withColumn('date_sub_date',date_sub('date',10)).withColumn('date_sub_time',date_sub('time',10)).show()

# COMMAND ----------

#calculating difference current_date and date values as well as current_timestamp and time values.

# COMMAND ----------

from pyspark.sql.functions import datediff,current_date,current_timestamp

# COMMAND ----------

help(datediff)

# COMMAND ----------

dtdf.withColumn('datediff_date',datediff(current_date(),'date')).withColumn('datediff_time',datediff(current_timestamp(),'time')).show()

# COMMAND ----------

#get the no of months between current_date and date values as well as current_timestamp and time values.
#add 3 months to both datevalue as well as time values

# COMMAND ----------

from pyspark.sql.functions import months_between,add_months,round

# COMMAND ----------

help(months_between)

# COMMAND ----------

help(add_months)

# COMMAND ----------

dtdf.show(truncate=False)

# COMMAND ----------

dtdf.withColumn('months_between_date',round(months_between(current_date(),'date'),2)).withColumn('months_between_time',round(months_between(current_timestamp(),'time'),2)).withColumn('add_month_date',add_months('date',3)).withColumn('add_month_time',add_months('time',3)).show(truncate=False)

# COMMAND ----------

#Date and TIme Trunc Functions

# COMMAND ----------

from pyspark.sql.functions import trunc,date_trunc

# COMMAND ----------

help(trunc)

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

dtdf.show(truncate=False)

# COMMAND ----------

dtdf.withColumn('date_trunc',trunc('date','MM')).withColumn('time_trunc',trunc('time','yy')).show(truncate=False)

# COMMAND ----------

#fetching beginning hour time using date and time field


# COMMAND ----------

from pyspark.sql.functions import date_trunc 

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

dtdf.withColumn('date_trunc',date_trunc('MM','date')).withColumn('time_trunc',date_trunc('yy','time')).show(truncate=False)

# COMMAND ----------

dtdf.withColumn('date_dt',date_trunc('HOUR','date')).withColumn('time_dt',date_trunc('HOUR','time')).withColumn('time_dt1',date_trunc('dd','time')).show(truncate=False)

# COMMAND ----------

#Date and Time Extract Functions

# COMMAND ----------

df.show()

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import year,month,weekofyear,dayofmonth,dayofyear,dayofweek,current_date

# COMMAND ----------

help(year)

# COMMAND ----------

df.select(current_date().alias('current-date'),year(current_date()).alias('year'),month(current_date()).alias('month'),weekofyear(current_date()).alias('weekofyear'),dayofyear(current_date()).alias('dayofyear'),dayofmonth(current_date()).alias('dayofmonth'),dayofweek(current_date()).alias('dayofweek')).show()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

help(F.current_timestamp)

# COMMAND ----------

df.select(F.current_timestamp().alias('current-timestamp'),F.year(F.current_timestamp()).alias('year'),F.month(F.current_timestamp()).alias('month'),F.dayofmonth(F.current_timestamp()).alias('dayofmonth'),F.hour(F.current_timestamp()).alias('hour'),F.minute(F.current_timestamp()).alias('minute'),F.second(F.current_timestamp()).alias('second')).show()

# COMMAND ----------

#Usage of to_date and to_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_date,to_timestamp,lit

# COMMAND ----------

datetimes=[(20140228,'28-Feb-2014 10:00:00.123'),(20160229,'20-Feb-2016 08:08:08.999'),(20171031,'31-Dec-2017 11:59:59.123')]

# COMMAND ----------

dtdf=spark.createDataFrame(datetimes,schema='date bigint, time string')

# COMMAND ----------

dtdf.show(truncate=False)

# COMMAND ----------

df.show()

# COMMAND ----------

help(to_date)

# COMMAND ----------

df.select(to_date(lit('20210302'),'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('20210302'),'yyyyddMM').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('2021002'),'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02/03/3021'),'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-03-3021'),'dd-MM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-Mar-3021'),'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-March-2021'),'dd-MMMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('March 2,2021'),'MMMM d,yyyy').alias('to_date')).show()

# COMMAND ----------



# COMMAND ----------

df.select(to_timestamp(lit('20210302'),'yyyyMMdd').alias('to_timestamp')).show()

# COMMAND ----------

df.select(to_timestamp(lit('20210302 17:20:15'),'yyyyMMdd HH:mm:ss').alias('to_timestamp')).show()

# COMMAND ----------

#converting data in datetimes to standard dates or timestamp

# COMMAND ----------

dtdf.printSchema()

# COMMAND ----------

 dtdf.withColumn('to_date',to_date(col('date').cast('string'),'yyyyMMdd')).withColumn('to_timestamp',to_timestamp(col('time'),'dd-MMM-yyyy HH:mm:ss.SSS')).show(truncate=False)

# COMMAND ----------

#Usage of date_format function

# COMMAND ----------

datetimes=[('2014-02-28','2014-02-28 10:00:00.123'),('2016-02-29','2016-02-29 08:08:08.999'),('2017-10-31','2017-10-31 11:59:59.123')]

# COMMAND ----------

dtdf=spark.createDataFrame(datetimes,schema='date string ,time string')

# COMMAND ----------

dtdf.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

help(date_format)

# COMMAND ----------

#getting the year and the month from both date and time columns using yyyyMM format. Also ensure that the data type is converted to integer.

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyMM')).withColumn('time_ym',date_format('time','yyyyMM')).show(truncate=False)

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyMM').cast('int')).withColumn('time_ym',date_format('time','yyyyMM').cast('int')).show(truncate=False)

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyMM').cast('int')).withColumn('time_ym',date_format('time','yyyyMM').cast('int')).printSchema()

# COMMAND ----------

#fetching the data in yyyyMMddHHmmss format

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyMMddHHmmss')).withColumn('time_ym',date_format('time','yyyyMMddHHmmss')).show(truncate=False)

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyMMddHHmmss').cast('long')).withColumn('time_ym',date_format('time','yyyyMMddHHmmss').cast('long')).show(truncate=False)

# COMMAND ----------

#fetching year and day of year using yyyyDD format.

# COMMAND ----------

dtdf.withColumn('date_ym',date_format('date','yyyyDDD').cast('int')).withColumn('time_ym',date_format('time','yyyyDDD').cast('int')).show()

# COMMAND ----------

#fetching complete description of the DATE

# COMMAND ----------

dtdf.withColumn('date_desc',date_format('date','MMMM d, yyyy')).show(truncate=False)

# COMMAND ----------

#getting the name of the week day using date.

# COMMAND ----------

dtdf.withColumn('day_name_abbr',date_format('date','EE')).show(truncate=False)

# COMMAND ----------

dtdf.withColumn('day_name_abbr',date_format('date','EEEE')).show(truncate=False)

# COMMAND ----------

#Dealing with Unix Timestamp

# COMMAND ----------

datetimes=[(20140228,'2014-02-28', '2014-02-28 10:00:00'),
 (20160229,'2016-02-29', '2016-02-29 08:08:08'),
 (20171031,'2017-10-31', '2017-10-31 11:59:59')]

# COMMAND ----------

 dtdf=spark.createDataFrame(datetimes,schema='dateid bigint, date string ,time string')

# COMMAND ----------

dtdf.show()

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

# COMMAND ----------

help(unix_timestamp)

# COMMAND ----------

 dtdf\
       .withColumn('unix_date_id',unix_timestamp(col('dateid').cast('string'),'yyyyMMdd'))\
       .withColumn('unix_date',unix_timestamp(col('date'),'yyyy-MM-dd'))\
    .withColumn('unix_time',unix_timestamp('time')).show(truncate=False)

# COMMAND ----------

#converting unixtimestamp to date time format back

# COMMAND ----------

unixtimes=[(1393581600,),(1456733288,),(1509451199,)]

# COMMAND ----------

 utdf=spark.createDataFrame(unixtimes).toDF('unixtime')

# COMMAND ----------

utdf.show()

# COMMAND ----------

utdf.printSchema()

# COMMAND ----------

#getting date in yyyyMMdd format and also complete timestamp

# COMMAND ----------

from pyspark.sql.functions import from_unixtime,col

# COMMAND ----------

help(from_unixtime)

# COMMAND ----------

utdf.withColumn('date',from_unixtime('unixtime','yyyyMMdd')).withColumn('time',from_unixtime('unixtime')).show()

# COMMAND ----------

help(col('unixtime').cast)

# COMMAND ----------

utdf.select(col('unixtime').cast('date')).show()

# COMMAND ----------

utdf.select(col('unixtime').cast('timestamp')).show()

# COMMAND ----------



# COMMAND ----------


