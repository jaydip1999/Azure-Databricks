# Databricks notebook source
#Creating Spark Dataframe

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

#Overview of Select on Spark Dataframe

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

users_df.select(['id','first_name','last_name']).show()

# COMMAND ----------

#defining alias to the dataframe
users_df.alias('u').select('*').show()

# COMMAND ----------

users_df.alias('u').select('u.*').show()

# COMMAND ----------

users_df.alias('u').select('u.id','u.first_name','u.last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select(col('id'),'first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

users_df.select(col('id'),'first_name','last_name',concat(col('first_name'),lit(', '),col('last_name')).alias('full_name')).show()

# COMMAND ----------

#Overview of selectExpr on Spark Dataframe

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

  users_df.selectExpr('*').show()

# COMMAND ----------

#defining alias
users_df.alias('u').selectExpr('u.*').show()

# COMMAND ----------

users_df.selectExpr('id','first_name','last_name').show()

# COMMAND ----------

users_df.select(['id','first_name','last_name']).show()

# COMMAND ----------

users_df.select('id','first_name','last_name',concat(col('first_name'),lit(', '),col('last_name')).alias('full_name')).show() 

# COMMAND ----------

#Using selecexpr to use Spark SQL functions
users_df.selectExpr('id','first_name','last_name',"concat(first_name,', ',last_name) as full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
             select id,first_name,last_name,concat(first_name,',',last_name) as full_name from users 
"""
).show()

# COMMAND ----------

#Referring Columns using Spark Dataframe Names  

# COMMAND ----------

users_df['id']

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

col('id')

# COMMAND ----------

type(users_df['id'])

# COMMAND ----------

type(col('id'))

# COMMAND ----------

 users_df.select(users_df['id'],col('first_name'),'last_name').show()

# COMMAND ----------

 users_df.alias('a').select(a['id'],col('first_name'),'last_name').show()

# COMMAND ----------

 users_df.alias('a').select(col('id'),col('first_name'),col('last_name')).show()

# COMMAND ----------

 users_df.alias('a').select('a.id',col('first_name'),'last_name').show()

# COMMAND ----------

 users_df.selectExpr('id',col('first_name'),'last_name').show()

# COMMAND ----------

 users_df.selectExpr('id',users_df['first_name'],'last_name').show()

# COMMAND ----------

users_df.select('id','first_name','last_name',concat(users_df['first_name'],lit(', '),col('last_name')).alias('full_name')).show() 

# COMMAND ----------

users_df.alias('a').selectExpr('id','first_name','last_name',"concat(a.first_name,', ',a.last_name) as full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')
spark.sql("""
             select id,first_name,last_name,concat(u.first_name,',',u.last_name) as full_name from users u
"""
).show()

# COMMAND ----------

#understanding col function in spark

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

l=['id','first_name','last_name']
users_df.select(*l).show()

# COMMAND ----------

help(col)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user_id=col('id')

# COMMAND ----------

user_id

# COMMAND ----------

users_df.select(user_id).show()

# COMMAND ----------

users_df.select('id','customer_from').show()

# COMMAND ----------

users_df.select('id','customer_from').dtypes

# COMMAND ----------

users_df.select(
col('id'),date_format('customer_from','yyyyMMdd').alias('customer_from')
).printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format
users_df.select(
col('id'),date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')
).show()

# COMMAND ----------

users_df.select(
col('id'),date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')
).printSchema()

# COMMAND ----------

cols=[col('id'),date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')] #list of column-type objects
users_df.select(*cols).show()

# COMMAND ----------

#Invoking Functions using Spark Column Objects

# COMMAND ----------

from pyspark.sql.functions import lit,concat

# COMMAND ----------

users_df.select('id','first_name','last_name',concat(col('first_name'),lit(', '),col('last_name')).alias('full_name')).show()

# COMMAND ----------



# COMMAND ----------

users_df.selectExpr('id','first_name','last_name',"concat(first_name,', ',last_name) as full_name").show()

# COMMAND ----------

#understandong lit function in Spark

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
        select id,amount_paid+25 as amount_paid from users
""").show()

# COMMAND ----------

users_df.selectExpr('id','amount_paid+25 as amount_paid').show()

# COMMAND ----------

users_df.select('id','amount_paid'+'25').show()

# COMMAND ----------

users_df.select('i'+'d',col('amount_paid')+lit(25)).show()

# COMMAND ----------

lit(25)

# COMMAND ----------

#Overview of Renaming Spark Dataframe Columns or Expressions

# COMMAND ----------

#Naming derived columns using withColumn

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id','first_name','last_name',concat('first_name',lit(', '),'last_name').alias('full_name')).show()

# COMMAND ----------

users_df.select('id','first_name','last_name').withColumn('full_name',concat('first_name',lit(', '),'last_name')).show()

# COMMAND ----------

help(users_df.withColumn)

# COMMAND ----------

users_df.select('id','first_name','last_name').withColumn('fn','full_name').show()#second argument should be function which returns column type object

# COMMAND ----------

users_df.select('id','courses').show()

# COMMAND ----------

from pyspark.sql.functions import size,concat,col,lit

# COMMAND ----------

users_df.select('id','courses').withColumn('course_count',size('courses')).show()

# COMMAND ----------

#Renaming columns using withColumnRenamed

# COMMAND ----------

help(users_df.withColumnRenamed)

# COMMAND ----------

users_df.select(col('id').alias('user_id'),'first_name','last_name').show()#alias only works with column type object

# COMMAND ----------

users_df.select('id','first_name','last_name').\
withColumnRenamed('id','user_id').\
withColumnRenamed('first_name','user_first_name').\
withColumnRenamed('last_name','user_last_name').\
show()

# COMMAND ----------

#Renaming Spark Dataframe columns or expressions using alias function


# COMMAND ----------

users_df.select(col('id').alias('user_id'),col('first_name').alias('user_first_name'),col('last_name').alias('user_last_name')).show()

# COMMAND ----------

users_df.select(users_df['id'].alias('user_id'),users_df['first_name'].alias('user_first_name'),users_df['last_name'].alias('user_last_name')).\
withColumn('user_full_name',concat(col('user_first_name'),lit(','),col('user_last_name'))).\
show()

# COMMAND ----------

users_df.withColumn('user_full_name',concat(col('first_name'),lit(','),col('last_name'))).\
         select(users_df['id'].alias('user_id'),users_df['first_name'].alias('user_first_name'),users_df['last_name'].alias('user_last_name'),'user_full_name').show()

# COMMAND ----------

users_df['first_name']users_df['last_name']

# COMMAND ----------

users_df.withColumn('user_full_name',concat(users_df['first_name'],lit(','),users_df['last_name'])).\
         select(users_df['id'].alias('user_id'),users_df['first_name'].alias('user_first_name'),users_df['last_name'].alias('user_last_name'),'user_full_name').show()

# COMMAND ----------

#Renaming and Rordering multiple Spark Dataframe Columns

# COMMAND ----------

#needed original columns
columns1=['id','first_name','last_name','email','phone_no','courses']
#updated columns
columns2=['user_id','user_first_name','user_last_name','user_email','user_phone_no','user_courses']

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

users_df.select(columns1).show()

# COMMAND ----------

users_df.select(columns1).toDF(*columns2).show()

# COMMAND ----------

def myDF(*cols):
    print(type(cols))
    print(cols)

# COMMAND ----------

myDF(*['f1','f2'])

# COMMAND ----------


