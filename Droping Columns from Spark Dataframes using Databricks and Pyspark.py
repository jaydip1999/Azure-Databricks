# Databricks notebook source
#Creating Spark Dataframe for Dropping Columns

# COMMAND ----------

from pyspark.sql import Row
from  pyspark.sql.functions import col

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
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
        'phone_no':Row(mobile='93427382623',home='93427382623') ,
        'courses':[3],
        'is_customer':True,
        'amount_paid':900.55,
        'customer_from':datetime.date(2021,2,14),
        'last_updated_is':datetime.datetime(2021,2,18,4,33,0)
    },
  {
      'id' :2,
        'first_name':'Vishal',
        'last_name':'Barvaliya',
        'email':'vishalbarvaliya@gmail.com',
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
        'phone_no':Row(mobile=None,home=None),
        'courses':[],
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    },
    {
      'id' :3,
        'first_name':'Bhavik',
        'last_name':'Dhaduk',
        'email':'bhavikgajera@gmail.com',
        'phone_no':Row(mobile=None,home=None),
        'courses':[],
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

import pandas as pd

# COMMAND ----------


spark.conf.set('spark.sql.execution.arrow.pyspark.enabled',False)

# COMMAND ----------

users_df=spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

#Introduction of drop function

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

help(users_df.drop)

# COMMAND ----------

#Dropping a single column

# COMMAND ----------

users_df.printSchema() 

# COMMAND ----------

users_df.drop('last_name').printSchema()

# COMMAND ----------

users_df.drop(users_df['last_name']).printSchema()

# COMMAND ----------

users_df.drop(col('last_name')).printSchema()




# COMMAND ----------

#if any column does not exist it does not throw error
users_df.drop(col('user_last_name')).printSchema()


# COMMAND ----------

#Dropping multiple columns at the same time

# COMMAND ----------

users_df.drop('last_name','first_name').printSchema()


# COMMAND ----------

#This throws error as we are passing multiple column type objects. So, we have to pass column names as strings if we want to delete multiple columns at the same time.
users_df.drop(col('last_name'),col('id')).printSchema()

# COMMAND ----------

#Dropping List of Columns from Spark Dataframe

# COMMAND ----------

columns=['id','first_name','city','email']

# COMMAND ----------

#We have to pass only varying arguments or strings in drop function.
users_df.drop(columns)

# COMMAND ----------

users_df.drop(*columns).show()

# COMMAND ----------

 #Dropping Duplicate Records from Spark Dataframes

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.count()

# COMMAND ----------

users_df.distinct().count()

# COMMAND ----------

users_df.distinct().show()

# COMMAND ----------

help(users_df.dropDuplicates)

# COMMAND ----------

help(users_df.drop_duplicates)

# COMMAND ----------

users_df.dropDuplicates().count()

# COMMAND ----------

users_df.drop_duplicates().count()

# COMMAND ----------

#we have to pass list or array otherwise it will throw error.
users_df.drop_duplicates('id').show()

# COMMAND ----------

users_df.drop_duplicates(['id']).show()

# COMMAND ----------

users_df.drop_duplicates(['id','last_name']).show()

# COMMAND ----------

#Dropping Null based Records from Spark Dataframes

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
        'is_customer':True,
        'amount_paid':1000.55,
        'customer_from':datetime.date(2021,1,15),
        'last_updated_is':datetime.datetime(2021,2,10,1,15,0)
    },
    {
      'id' :None,
        'first_name':None,
        'last_name':None,
        'email':None,
        'is_customer':None,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':None
    },
    {
      'id' :2,
        'first_name':'Vishal',
        'last_name':'Barvaliya',
        'email':'vishalbarvaliya@gmail.com',
        'is_customer':True,
        'amount_paid':900.55,
        'customer_from':datetime.date(2021,2,14),
        'last_updated_is':datetime.datetime(2021,2,18,4,33,0)
    },
  {
      'id' :2,
        'first_name':'Vishal',
        'last_name':'Barvaliya',
        'email':'vishalbarvaliya@gmail.com',
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
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    },
    {
      'id' :3,
        'first_name':'Bhavik',
        'last_name':'Dhaduk',
        'email':'bhavikgajera@gmail.com',
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    },
    {
      'id' :4,
        'first_name':'Dhaval',
        'last_name':'Kathiriya',
        'email':'kathiriyadhaval@gmail.com',
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,2,10,1,15,2)
    },
    {
      'id' :4,
        'first_name':'Dhaval',
        'last_name':'Kathiriya',
        'email':'kathiriyadhaval@gmail.com',
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,2,10,1,15,2)
    },
    {
      'id' :5,
        'first_name':'Krutik',
        'last_name':'Kariya',
        'email':'krutikkariya@gmail.com',
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,2,10,1,15,1)
    },
    {
      'id' :None,
        'first_name':None,
        'last_name':None,
        'email':None,
        'is_customer':None,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':None
    },
    {
      'id' :5,
        'first_name':None,
        'last_name':None,
        'email':None,
        'is_customer':None,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':None
    },
    {
      'id' :None,
        'first_name':None,
        'last_name':None,
        'email':'soham@gmail.com',
        'is_customer':None,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':None
    },
    {
      'id' :None,
        'first_name':'Soham',
        'last_name':'Arpana',
        'email':None,
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2022,2,10,1,15,1)
    }
]

# COMMAND ----------

users_schema='''
    id string,
    first_name string,
    last_name string,
    email string,
    is_customer boolean,
    amount_paid string,
    customer_from date,
    last_updated_is timestamp
'''

# COMMAND ----------

users_df=spark.createDataFrame(users,schema=users_schema)

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.count()

# COMMAND ----------

help(users_df.dropna)

# COMMAND ----------

users_df.dropna(thresh=5).show()

# COMMAND ----------

users_df.dropna('any').show()

# COMMAND ----------

users_df.dropna('all').show()

# COMMAND ----------

users_df.dropna('all',subset=['id','email']).show()

# COMMAND ----------

users_df.dropna('any',subset=['id','email']).show()

# COMMAND ----------


