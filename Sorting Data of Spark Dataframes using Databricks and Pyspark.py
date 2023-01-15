# Databricks notebook source
#Creating Spark Dataframe for Sorting Data

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
      'id' :3,
        'first_name':'Bhavik',
        'last_name':'Gajera',
        'email':'bhavikgajera@gmail.com',

        'phone_no':Row(mobile='93427382623',home='93427382623') ,
        'courses':[2,4],
        'is_customer':True,
        'amount_paid':850.55,
        'customer_from':datetime.date(2021,2,21),
        'last_updated_is':datetime.datetime(2021,4,2,0,0,55,18)
    },
    {
         'id' :4,
        'first_name':'Dhaval',
        'last_name':'Kathiriya',
        'email':'dhavalkathiriya@gmail.com',

        'phone_no':Row(mobile=None,home=None),
        'courses':[],
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,2,10,1,15,0)
    },
    {
         'id' :5,
        'first_name':'Krutik',
        'last_name':'Shiroya',
        'email':'KrutikShiroya@gmail.com',

        'phone_no':Row(mobile='93427382623',home='93427382623'),
        'courses':[],
        'is_customer':False,
        'amount_paid':None,
        'customer_from':None,
        'last_updated_is':datetime.datetime(2021,2,10,1,15,0)
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

help(users_df.sort)

# COMMAND ----------

help(users_df.orderBy)

# COMMAND ----------

#Sort Spark Dataframe in Ascending Order by a given column 

# COMMAND ----------

users_df.show()

# COMMAND ----------

#sorting the data in ascending order by first_name column

# COMMAND ----------

users_df.sort('first_name').show()

# COMMAND ----------

users_df.sort(users_df.first_name).show()

# COMMAND ----------

users_df.sort(users_df['first_name']).show()

# COMMAND ----------

from pyspark.sql.functions  import col 

# COMMAND ----------

users_df.sort(col('first_name')).show()

# COMMAND ----------


#sorting the data in ascending order by customer_from

# COMMAND ----------

users_df.sort(col('customer_from')).show()

# COMMAND ----------


#sorting the data in ascending order by no of enrolled courses

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df.sort(size('courses')).show()

# COMMAND ----------

users_df.select('id','courses').withColumn('no_of_courses',size('courses')).sort('no_of_courses').show()

# COMMAND ----------

  #Sort Spark Dataframe in Descending Order by a given column 

# COMMAND ----------

#sorting the data in Descending order by first_name column


# COMMAND ----------

users_df.sort('first_name',ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'],ascending=False).show()

# COMMAND ----------

users_df.sort(users_df['first_name'].desc()).show()

# COMMAND ----------

users_df.sort(col('first_name').desc()).show()

# COMMAND ----------

from  pyspark.sql.functions import desc

# COMMAND ----------

users_df.sort(desc('first_name')).show()

# COMMAND ----------

#sorting the data in descending order by customer_from column


# COMMAND ----------

users_df.sort('customer_from',ascending=False).show()

# COMMAND ----------

#sorting the data in Descending order by no of enrolled courses column

# COMMAND ----------

from  pyspark.sql.functions import size

# COMMAND ----------

users_df.select('id','courses').withColumn('no_of_courses',size('courses')).sort('no_of_courses',ascending=False).show()

# COMMAND ----------

#Dealing with Nulls while sorting Spark Dataframe

# COMMAND ----------

#sorting the data in ascending order by customer_from column

# COMMAND ----------

users_df.select('id','customer_from').orderBy('customer_from').show()

# COMMAND ----------

#sorting the data in ascending order by customer_from column with null values at the end

# COMMAND ----------

users_df.select('id','customer_from').orderBy(col('customer_from').asc_nulls_last()).show()

# COMMAND ----------

#sorting the data in descending order by customer_from column

# COMMAND ----------

users_df.select('id','customer_from').orderBy(col('customer_from').desc()).show()

# COMMAND ----------

#sorting the data in descending order by customer_from column with null values at the starting position

# COMMAND ----------

users_df.select('id','customer_from').orderBy(col('customer_from').desc_nulls_first()).show()

# COMMAND ----------

#Composite Sorting  

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

 #sorting email in ascending order by first_name and then in ascending order by last_name

# COMMAND ----------

 #sorting email in ascending order by first_name and then in deqaqscending order by last_name
