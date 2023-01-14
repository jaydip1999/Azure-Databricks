# Databricks notebook source
#Creating Spark Dataframe for Filtering 

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
        'amount_paid':None,
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

#Overview of Filter or Where Function on Spark Dataframe

# COMMAND ----------

help(users_df.filter)

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,filter

# COMMAND ----------

users_df.filter(col('id')==1).show()

# COMMAND ----------

users_df.where(col('id')==1).show()

# COMMAND ----------

users_df.filter('id=1').show()

# COMMAND ----------

users_df.where('id=1').show()

# COMMAND ----------

users_df.filter('id==1').show()

# COMMAND ----------

users_df.where('id==1').show()

# COMMAND ----------

users_df.where(users_df['id']==1).show()

# COMMAND ----------

users_df.filter(users_df['id']==1).show()

# COMMAND ----------

users_df.where(users_df.id==1).show()

# COMMAND ----------

users_df.filter(users_df.id==1).show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql(""" select * from users where id = 1 """).show()

# COMMAND ----------

#Overview of Conditions and Operators

# COMMAND ----------

#Filter using Equal Condition

# COMMAND ----------

#getting list of customers(is_customer flag is set to true)
users_df.filter(col('is_customer')==True).show()

# COMMAND ----------

users_df.filter(col('is_customer')=='true').show()

# COMMAND ----------

users_df.filter('is_customer = true').show()

# COMMAND ----------

users_df.filter('is_customer = "true"').show()

# COMMAND ----------

users_df.filter('is_customer = True').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql(""" select * from users where is_customer = True """).show()

# COMMAND ----------

spark.sql(""" select * from users where is_customer = 'true' """).show()

# COMMAND ----------

#fetching users from Toronto city

# COMMAND ----------

users_df.filter("city=='Toronto'").show()

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat
users_df.filter(col('city')=='Toronto').show()

# COMMAND ----------

#fetching customers who paid 900.55

# COMMAND ----------

users_df.filter(col('amount_paid')==900.55).show()

# COMMAND ----------

users_df.filter(col('amount_paid')=='900.55').show()

# COMMAND ----------

users_df.filter("amount_paid =='900.55'").show()

# COMMAND ----------

users_df.filter("amount_paid = 900.55 ").show()

# COMMAND ----------

#fetching customers  where paid amount is not a number

# COMMAND ----------

from pyspark.sql.functions import isnan

# COMMAND ----------

users_df.select('amount_paid',isnan('amount_paid')).show()

# COMMAND ----------

users_df.filter(isnan('amount_paid')==True).show()

# COMMAND ----------

#Filter using Not Equal Condition

# COMMAND ----------

#fetching users who are not living in Toronto city

# COMMAND ----------

users_df.select('id','city').show()

# COMMAND ----------

users_df.select('id','city').filter(col('city')!='Toronto').show()

# COMMAND ----------

users_df.select('id','city').filter((col('city')!='Toronto') | (col('city').isNull())).show()

# COMMAND ----------

users_df.select('id','city').filter((col('city')!='')).show()

# COMMAND ----------

#Filter using Between operator

# COMMAND ----------

#fetching user_id and email whose last updated timestamp is between 2021 feb 15th and 2021 march 15th

# COMMAND ----------

users_df.select('id','email','last_updated_is').filter(col('last_updated_is').between('2021-02-15 00:00:00','2021-03-15 23:59:59')).show()

# COMMAND ----------

#fetching users whose payment is in the range of 850 and 900.

# COMMAND ----------

users_df.select('id','amount_paid').show()

# COMMAND ----------

users_df.select('id','amount_paid').filter(col('amount_paid').between(900,1100)).show()

# COMMAND ----------

#Dealing with Null values While Filtering


# COMMAND ----------

#fetching users whose city is not null

# COMMAND ----------

users_df.select('id','city').show()

# COMMAND ----------

users_df.select('id','city').filter(col('city').isNotNull()).show()

# COMMAND ----------

users_df.select('id','city').filter("city is not null").show()

# COMMAND ----------

#fetching users whose city is null

# COMMAND ----------

users_df.select('id','city').filter(col('city').isNull()).show()

# COMMAND ----------

users_df.select('id','city').filter("city is null").show()

# COMMAND ----------

#fetching users whose customer_from is null

# COMMAND ----------

users_df.select('id','customer_from').show()

# COMMAND ----------

users_df.select('id','city').filter(col('customer_from').isNull()).show()

# COMMAND ----------

users_df.select('id','city').filter("customer_from is null").show()

# COMMAND ----------

#Overview of Boolean Operations

# COMMAND ----------

#Boolean OR on same column of Spark Dataframe and In operator

# COMMAND ----------

from pyspark.sql.functions import col  

# COMMAND ----------

#fetching list of users whose city is null or empty string(users with no cities associated)

# COMMAND ----------


