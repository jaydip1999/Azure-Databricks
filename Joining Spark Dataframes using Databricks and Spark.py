# Databricks notebook source
#Setup Datasets to perform Joins

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

import datetime
courses=[
    {
        'course_id':1,
        'course_title':'Mastering Python',
        'course_published_git':datetime.date(2021,1,14),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':2,
        'course_title':'Data Engineering Essentials',
        'course_published_git':datetime.date(2021,2,10),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':3,
        'course_title':'Mastering Python',
        'course_published_git':datetime.date(2021,1,7),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':4,
        'course_title':'AWS Essentials',
        'course_published_git':datetime.date(2021,3,19),
        'is_active':False,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    },
    {
        'course_id':5,
        'course_title':'Docker 101',
        'course_published_git':datetime.date(2021,2,28),
        'is_active':True,
        'last_updated_ts':datetime.datetime(2021,2,2,18,16,57,25)
    }
]

courses_df=spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

users=[
    {
        'user_id':1,
        'user_first_name':'Jaydip',
        'user_last_name':'Dobariya',
        'user_email':'dobariyajaydip@gmail.com'
    },
    {
        'user_id':2,
        'user_first_name':'Vishal',
        'user_last_name':'Barvaliya',
        'user_email':'barvaliyavishal@gmail.com'
    },
    {
        'user_id':3,
        'user_first_name':'Bhavik',
        'user_last_name':'Gajera',
        'user_email':'gajerabhavik@gmail.com'
    },
    {
        'user_id':4,
        'user_first_name':'Dhaval',
        'user_last_name':'Kathiriya',
        'user_email':'kathiriyadhaval@gmail.com'
    },
    {
        'user_id':5,
        'user_first_name':'Meet',
        'user_last_name':'Ambaliya',
        'user_email':'ambaliyameet@gmail.com'
    },
    {
        'user_id':6,
        'user_first_name':'Shyam',
        'user_last_name':'Kaveri',
        'user_email':'kaverishyam@gmail.com'
    },
    {
        'user_id':7,
        'user_first_name':'Krutik',
        'user_last_name':'Shiroya',
        'user_email':'shiroyakrutik@gmail.com'
    },
    {
        'user_id':8,
        'user_first_name':'Jenish',
        'user_last_name':'Thummar',
        'user_email':'thummarjenish@gmail.com'
    },
    {
        'user_id':9,
        'user_first_name':'Sanket',
        'user_last_name':'Bhimani',
        'user_email':'bhimanisanket@gmail.com'
    },
    {
        'user_id':10,
        'user_first_name':'Jay',
        'user_last_name':'Chothani',
        'user_email':'jaychothani@gmail.com'
    }
]
users_df=spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

course_enrolments=[
    {
        'course_enrolment_id':1,
        'user_id':10,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':2,
        'user_id':5,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':3,
        'user_id':7,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':4,
        'user_id':9,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':5,
        'user_id':8,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':6,
        'user_id':5,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':7,
        'user_id':4,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':8,
        'user_id':7,
        'course_id':3,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':9,
        'user_id':8,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':10,
        'user_id':3,
        'course_id':3,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':11,
        'user_id':7,
        'course_id':5,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':12,
        'user_id':3,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':13,
        'user_id':5,
        'course_id':2,
        'price_paid':9.99
    },
    {
        'course_enrolment_id':14,
        'user_id':4,
        'course_id':3,
        'price_paid':10.99
    },
    {
        'course_enrolment_id':15,
        'user_id':8,
        'course_id':2,
        'price_paid':9.99
    }
]
course_enrolments_df=spark.createDataFrame([Row(**user) for user in course_enrolments])

# COMMAND ----------

courses_df.show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

course_enrolments_df.show()

# COMMAND ----------

#Overview of Joins 

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

#Explaining Aliases

# COMMAND ----------

help(courses_df.alias)

# COMMAND ----------

type(courses_df.alias('c'))

# COMMAND ----------

courses_df.alias('c').select('c.course_id').show()

# COMMAND ----------

courses_df.alias('c').select('c.*').show()

# COMMAND ----------

#Inner Join

# COMMAND ----------

help(courses_df.join)

# COMMAND ----------

#Fetching users details who have enrolled for the courses.
        

# COMMAND ----------

users_df.join(course_enrolments_df,users_df.user_id==course_enrolments_df.user_id).show()

# COMMAND ----------

#we can pass the common column between tables like this. In this case, that column is not repeated second time.
users_df.join(course_enrolments_df,'user_id').show()

# COMMAND ----------

users_df.join(course_enrolments_df,users_df.user_id==course_enrolments_df['user_id'].select(users_df['*'],course_enrolments_df.course_id,course_enrolments_df['course_enrolment_id']).show()

# COMMAND ----------


