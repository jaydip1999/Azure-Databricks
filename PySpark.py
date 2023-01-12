# Databricks notebook source
spark

# COMMAND ----------

age = [10,11,15,13,17]

# COMMAND ----------

spark.createDataFrame(age, 'int').show()

# COMMAND ----------


