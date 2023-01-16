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

users_df.join(course_enrolments_df,users_df.user_id==course_enrolments_df['user_id']).select(users_df['*'],course_enrolments_df.course_id,course_enrolments_df['course_enrolment_id']).show()

# COMMAND ----------

#alias use


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.alias('u').join(course_enrolments_df.alias('ce'),col('u.user_id')==course_enrolments_df['user_id']).select('u.*',col('course_id'),col('course_enrolment_id')).show()

# COMMAND ----------

users_df.alias('u').join(course_enrolments_df.alias('ce'),users_df.user_id==course_enrolments_df.user_id).groupBy('user_id').count().show()  

# COMMAND ----------

users_df.alias('u').join(course_enrolments_df.alias('ce'),users_df.user_id==course_enrolments_df.user_id).groupBy('u.user_id').count().show()  

# COMMAND ----------

users_df.alias('u').join(course_enrolments_df.alias('ce'),'user_id').groupBy('u.user_id').count().show()  

# COMMAND ----------

#Outer Join

# COMMAND ----------

#fetching all the user details along with course enrolment details (if user have any course enrolments)
#If any user does not have any course enrolments, we need to get all user details. Course details will be substituted with null values.
# for this we are going to do left join between users_df and course_enrolments_df and display all fields from users_df and course_id and course_enrolment_id  from course_enrolments tables. 

# COMMAND ----------

users_df.join(course_enrolments_df,users_df.user_id==course_enrolments_df.user_id,'left').show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').show() #we can write left/leftouter/left_outer/

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').select(users_df['*'],course_enrolments_df['course_id'],course_enrolments_df['course_enrolment_id']).show()

# COMMAND ----------

#fetching the all user details who are not enrolled in any course.
users_df.join(course_enrolments_df.alias('ce'),'user_id','left').filter('ce.course_enrolment_id is null').select(users_df['*'],course_enrolments_df['course_id'],course_enrolments_df['course_enrolment_id']).show()

# COMMAND ----------

#fetching no of courses enrolled by each user. If there are no enrolments, could them as '0'.
users_df.alias('u').join(course_enrolments_df.alias('ce'),'user_id','outer').groupBy('u.user_id').count().orderBy('u.user_id').show()  

# COMMAND ----------

from pyspark.sql.functions import sum, when

users_df.alias('u').join(course_enrolments_df.alias('ce'),'user_id','outer').groupBy('u.user_id').agg(sum(when(course_enrolments_df.course_enrolment_id.isNull(),0).otherwise(1)).alias('course_count')).orderBy('u.user_id').show()  

#.count().show()   

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

users_df.alias('u').join(course_enrolments_df.alias('ce'),'user_id','outer').groupBy('u.user_id').\
agg(sum(expr('''
                case when ce.course_enrolment_id is null then 0 else 1 end
                ''')    
).alias('course_count')).\
orderBy('u.user_id').show()  


# COMMAND ----------

#Right Outer Join

# COMMAND ----------

course_enrolments_df.join(users_df,'user_id','right').show()

# COMMAND ----------

course_enrolments_df.join(users_df,users_df.user_id==course_enrolments_df.user_id,'right').show()

# COMMAND ----------

course_enrolments_df.show()

# COMMAND ----------

#FULL_OUTER_JOIN

# COMMAND ----------

course_enrolments_df.join(users_df,'user_id','FULL').show()

# COMMAND ----------

course_enrolments_df.join(users_df,'user_id','FULL').count()

# COMMAND ----------

course_enrolments_df.join(users_df,'user_id','left').union(course_enrolments_df.join(users_df,'user_id','right')).count()

# COMMAND ----------

course_enrolments_df.join(users_df,'user_id','left').union(course_enrolments_df.join(users_df,'user_id','right')).distinct().count()

# COMMAND ----------

#Overview of Broadcast Join

# COMMAND ----------

spark.conf.get('spark.sql.autoBroadcastJoinThreshold')

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold','0')

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold','10485760b')

# COMMAND ----------

stream=spark.read.csv('',sep='\t',header=True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


