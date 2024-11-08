# Databricks notebook source
#Importing everything required for spark sql

import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

#Making the dataframe from the table that was created from the file

df_hum = spark.read.option("header",True).csv("/FileStore/tables/hum.csv")

# COMMAND ----------

#Transform column date from String type to Date type

df_hum = df_hum.withColumn('Date',to_date('Date'))

# COMMAND ----------

#Transform column value from String type to Double type

df_hum = df_hum.withColumn("Value",col("Value").cast('double'))

# COMMAND ----------

#Create a new column where the values are the months extracted from the Date column and drop the Date column

df_hum = df_hum.withColumn("Month",month("Date"))

# COMMAND ----------

#Calculate the standard deviation for each month, order them in descending order and keep the first row which is the one with the largest standard deviation value

q_hum = df_hum.groupBy("Month").agg({'Value': 'stddev'}).withColumnRenamed('stddev(Value)','Standard deviation').orderBy(desc('Standard deviation')).limit(1)

# COMMAND ----------

#Print the results

print("The month with the largest standard deviation is: ")
q_hum.printSchema()
q_hum.show()



