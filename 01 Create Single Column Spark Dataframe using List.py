# Databricks notebook source
#Create Single Column Spark Dataframe using Python List

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

#Create Multi Column Spark Dataframe using Python List

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

#Overview of Row

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

#Convert Lists of Lists into Spark Dataframe using Row

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

##Convert Lists of Tuples into Spark Dataframe using Row

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

#Convert Lists of Dictionaries into Spark Dataframe using Row

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

# COMMAND ----------

user_details=users_list[1]

# COMMAND ----------

user_details

# COMMAND ----------

Row(*user_details.values())

# COMMAND ----------

users_rows=[Row(*user.values()) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows,'user_id int,user_first_name string')

# COMMAND ----------

users_rows=[Row(**user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows)

# COMMAND ----------

def dummy(**kwargs):
    print(kwargs)
    print(len(kwargs))

# COMMAND ----------

users_details={'user_id':1,'user_first_name':'Jaydip'}


# COMMAND ----------

dummy(users_details=users_details)

# COMMAND ----------

dummy(user_id= 1, user_first_name= 'Jaydip')

# COMMAND ----------

dummy(**users_details)

# COMMAND ----------

#Overview of Basic Data Types in Spark

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
    }
]

# COMMAND ----------

from pyspark.sql import Row


# COMMAND ----------

users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

users_df

# COMMAND ----------

#Specifying Schema as String

# COMMAND ----------

import datetime
users=[
    (
      1,
        'Jaydip',
        'Dobariya',
       'dobariyajaydip@gmail.com',
        True,
        1000.55,
        datetime.date(2021,1,15),
        datetime.datetime(2021,2,10,1,15,0)
    ),
    (
      2,
        'Vishal',
        'Barvaliya',
        'vishalbarvaliya@gmail.com',
        True,
        900.55,
        datetime.date(2021,2,14),
        datetime.datetime(2021,2,18,4,33,0)
    ),
    (
      3,
        'Bhavik',
        'Gajera',
        'bhavikgajera@gmail.com',
        False,
        None,
        None,
        datetime.datetime(2021,4,2,0,0,55,18)
        )
]

# COMMAND ----------

users_schema='''
    id int,
    first_name string,
    last_name string,
    email string,
    is_customer boolean,
    amount_paid float,
    customer_from date,
    last_updated_ts timestamp
'''

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema)

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema).show()

# COMMAND ----------

#Specifying Schema as for Spark Dataframe using List

# COMMAND ----------

users_schema=[
    'id',
    'first_name',
    'last_name',
    'email',
    'is_customer' ,
    'amount_paid' ,
    'customer_from',
    'last_updated_ts' 
]

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema)

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema).show()

# COMMAND ----------

#Specifying Schema using Spark Types

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schema=StructType(
[StructField('id',IntegerType()),
StructField('first_name',StringType()),
StructField('last_name',StringType()),
StructField('email',StringType()),
 StructField('is_customer',BooleanType()),
StructField('amount_paid',FloatType()),
 StructField('customer_from',DateType()),
StructField('last_updated_ts',TimestampType())])

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema)

# COMMAND ----------

type(users_schema)

# COMMAND ----------

spark.createDataFrame(users,schema=users_schema).show()

# COMMAND ----------

#Create Spark Dataframe using Pandas Dataframe

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
        'amount_paid':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd.DataFrame(users)

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(users)).show()

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(users)).printSchema()

# COMMAND ----------

#Overview of Spark Special Data Types

# COMMAND ----------

#Array Type Columns in Spark Dataframes

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
        'phone_no':['93427382623','93427382623'],
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
        'phone_no':['93427382623','93427382623'],
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
        'phone_no':None,
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id','phone_no').show(truncate=False)

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

users_df.withColumn('phone',explode('phone_no')).drop('phone_no').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select('id',col('phone_no')[0].alias('mobile'),col('phone_no')[1].alias('home')).show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer

# COMMAND ----------

users_df.withColumn('phone',explode_outer('phone_no')).drop('phone_no').show()

# COMMAND ----------

#Map Type Columns in Spark Dataframes

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
        'phone_no':{'mobile':'93427382623','home':'93427382623'},
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
        'phone_no':{'mobile':'93427382623','home':'93427382623'} ,
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
        'phone_no':None,
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id','phone_no').show(truncate=False)

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select('id',col('phone_no')['mobile'].alias('mobile number')).show()

# COMMAND ----------

users_df.select('id',col('phone_no')['mobile'].alias('mobile number'),col('phone_no')['home'].alias('home number')).show()

# COMMAND ----------

users_df.select('id',explode('phone_no')).show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer

# COMMAND ----------

users_df.select('id',explode_outer('phone_no')).show()

# COMMAND ----------

users_df.select('*',explode('phone_no')).withColumnRenamed('key','phone_type').withColumnRenamed('value','phone_number').drop('phone_no').show()

# COMMAND ----------

#Struct Type Columns in Spark Dataframes

# COMMAND ----------

import datetime
users=[
    {
      'id' :1,
        'first_name':'Jaydip',
        'last_name':'Dobariya',
        'email':'dobariyajaydip@gmail.com',
        'phone_no':Row(mobile='93427382623',home='93427382623'),
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
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    }
]

# COMMAND ----------

users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

  users_df.select('id','phone_no').show(truncate=False)

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

users_df.select('id','phone_no.mobile','phone_no.home').show()

# COMMAND ----------

users_df.select('id','phone_no.*').show()

# COMMAND ----------

users_df.select('id',col('phone_no')['mobile'],col('phone_no')['home']).show()

# COMMAND ----------


