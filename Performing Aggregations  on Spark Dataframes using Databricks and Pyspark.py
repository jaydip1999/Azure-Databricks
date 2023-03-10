# Databricks notebook source
#Creating Spark Dataframe for aggregation

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
        'gender':'Male',
        'city':'Toronto',
        'phone_no':Row(mobile='93427382623',home='93427382623'),
        'courses':[1,2],
        'is_customer':True,
        'amount_paid':1000.55,
        'customer_from':datetime.date(2021,1,15),
        'last_updated_is':datetime.datetime(2021,2,10,1,15,0)
    },
    {
      'id' :2,
        'first_name':'Vishal',
        'last_name':'Barvaliya',
        'email':'vishalbarvaliya@gmail.com',
        'gender':'Male',
        'city':'Mississauga',
        'phone_no':Row(mobile='93427382623',home='93427382623') ,
        'courses':[3],
        'is_customer':True,
        'amount_paid':900.55,
        'customer_from':datetime.date(2021,2,14),
        'last_updated_is':datetime.datetime(2021,2,18,4,33,0)
    },
    {
      'id' :3,
        'first_name':'Bhavik',
        'last_name':'Gajera',
        'email':'bhavikgajera@gmail.com',
        'gender':'Female',
        'city':None,
        'phone_no':Row(mobile=None,home=None),
        'courses':[],
        'is_customer':False,
        'amount_paid':800,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled',False)


# COMMAND ----------

import pandas as pd

# COMMAND ----------

users_df=spark.createDataFrame(pd.DataFrame(users))


# COMMAND ----------

users_df.show()

# COMMAND ----------

#Some common aggregated functions

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

help(count)

# COMMAND ----------

users_df.select(count('*')).show()

# COMMAND ----------

users_df.groupBy('gender').agg(count('*')).show()

# COMMAND ----------

users_df.groupBy('gender').count().show()

# COMMAND ----------

#Total Aggregations on a Spark Dataframes 

# COMMAND ----------

#Get total of amount_paid for male customers

# COMMAND ----------

users_df.filter("gender='Male'").show()

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

help(sum)

# COMMAND ----------

users_df.filter("gender='Male'").select(sum('amount_paid').alias('total_amount_paid')).show()

# COMMAND ----------

#getting total of amount_paid as well as total no of customers for Male category

# COMMAND ----------

users_df.filter("gender='Male'").select(sum('id').alias('total_customers'),sum('amount_paid').alias('total_amount_paid')).show()

# COMMAND ----------

#Fetching Count of a Spark Dataframe

# COMMAND ----------

users_df.count()

# COMMAND ----------

type(users_df.count())

# COMMAND ----------

#it does not take action until we apply show command
users_df.select(count('*'))

# COMMAND ----------

users_df.select(count('*')).show()

# COMMAND ----------

#Overview of groupBy on Spark Dataframe

# COMMAND ----------

help(users_df.groupBy)

# COMMAND ----------

users_df.groupBy().min().show()

# COMMAND ----------

users_df.groupBy().count().show()

# COMMAND ----------

users_df.groupBy().sum().show()

# COMMAND ----------

#Perform Grouped Aggregations using direct functions on a Spark Dataframe

# COMMAND ----------

users_groupby=users_df.groupBy('gender')

# COMMAND ----------

type(users_groupby)

# COMMAND ----------

users_groupby.count().show()

# COMMAND ----------

users_groupby.count().withColumnRenamed('count','count_by_gender').show()

# COMMAND ----------

#get sum of all numeric columns
users_groupby.sum().show()

# COMMAND ----------

users_grouped=users_df.select('gender','id','amount_paid').groupBy('gender')

# COMMAND ----------

help(users_grouped.sum)

# COMMAND ----------

users_grouped.sum().show()

# COMMAND ----------

 
users_grouped.sum('id','amount_paid').show()

# COMMAND ----------

users_grouped.sum('amount_paid').show()

# COMMAND ----------

#Renaming columns
users_grouped.sum('id','amount_paid').toDF('category','customer_id','sum_of_amount_paid').show()

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

#Round to 0 numbers after '.'
users_grouped.sum('id','amount_paid').toDF('category','customer_id','sum_of_amount_paid').withColumn('perfect_sum_of_amount_paid',round('sum_of_amount_paid',0)).show()

# COMMAND ----------

#Perform Grouped Aggregations using agg functions on a Spark Dataframe

# COMMAND ----------

users_grouped=users_df.groupBy('gender')

# COMMAND ----------

type(users_grouped)

# COMMAND ----------

users_grouped.sum('id','amount_paid').show()

# COMMAND ----------

help(users_grouped.agg)

# COMMAND ----------

users_grouped.agg(sum('id'),sum('amount_paid')).show()

# COMMAND ----------

users_grouped.agg(sum('id').alias('customer_id'),round(sum('amount_paid'),0).alias('perfect_amount_paid')).show()

# COMMAND ----------

#passing aggregate funs as a dictionary into agg function
users_grouped.agg({'id':'sum','amount_paid':'sum'}).show()

# COMMAND ----------

users_grouped.agg({'id':'sum','amount_paid':'sum'}).toDF('category','sum_of_amount_paid','customer_id').withColumn('perfect_sum_of_amount_paid',round('sum_of_amount_paid',0)).show()

# COMMAND ----------

users_grouped.agg({'amount_paid':'sum','amount_paid':'min'}).show()

# COMMAND ----------

from pyspark.sql.functions import min 

# COMMAND ----------

users_grouped.agg(
    sum('id').alias('customer_id'),
    min('id').alias('min_customer_id'),
    round(sum('amount_paid'),0).alias('perfect_amount_paid'),
    min('amount_paid').alias('min_amount_paid')
).show()

# COMMAND ----------


