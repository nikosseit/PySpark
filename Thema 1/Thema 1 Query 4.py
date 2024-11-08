# Databricks notebook source
#Importing everything required for spark sql

import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

#Making the dataframes from the tables that were created from the files

df_hum = spark.read.option("header",True).csv("/FileStore/tables/hum.csv")
df_temp = spark.read.option("header",True).csv("/FileStore/tables/tempm-5.csv")

# COMMAND ----------

#Transform column date from String type to Date type

df_hum = df_hum.withColumn('Date',to_date('Date'))
df_temp = df_temp.withColumn('Date',to_date('Date'))

# COMMAND ----------

#Transform column value from String type to Double type

df_hum = df_hum.withColumn("Value",col("Value").cast('double'))
df_temp = df_temp.withColumn("Value",col("Value").cast('double'))

# COMMAND ----------

#Extract minimum and maximum values for each day and produce a dataframe that contains them

temp_min = df_temp.groupBy("Date").min("Value")
temp_min = temp_min.withColumn("Minimum temp",col("min(Value)"))
temp_max = df_temp.groupBy("Date").max("Value")
temp_max = temp_max.withColumn("Maximum temp",col("max(Value)"))
temp_val = (temp_min.join(temp_max, "Date").orderBy("Date")).drop("min(Value)","max(Value)")

hum_min = df_hum.groupBy("Date").min("Value")
hum_min = hum_min.withColumn("Minimum hum",col("min(Value)"))
hum_max = df_hum.groupBy("Date").max("Value")
hum_max = hum_max.withColumn("Maximum hum",col("max(Value)"))
hum_val = (hum_min.join(hum_max, "Date").orderBy("Date")).drop("min(Value)","max(Value)")

df = hum_val.join(temp_val, "Date").orderBy("Date")

# COMMAND ----------

#Calculate the minimum and maximum DI for each day and make two new columns that contain each value

df = df.withColumn("DI Minimum",col('Minimum temp') - 0.55*(1-0.01*col('Minimum hum'))*(col('Minimum temp') - 14.5))
df = df.withColumn("DI Maximum",col('Maximum temp') - 0.55*(1-0.01*col('Maximum hum'))*(col('Maximum temp') - 14.5))
df = df.drop('Minimum temp', 'Maximum temp', 'Minimum hum', 'Maximum hum')

# COMMAND ----------

#Create a dataframe that contains the minimum DI values and a dataframe that contains the maximum DI values to calculate min and max for each by limiting to the first entry

max_val = df.drop('DI Minimum').orderBy(desc('DI Maximum')).limit(1)
min_val = df.drop('DI Maximum').orderBy('DI Minimum').limit(1)

# COMMAND ----------

#Print dataframes that contain the results

print('The first 5 rows of the datafrane that contains the minimum and maximum DI values for each date are: ')
df.printSchema()
df.show(5)

print('The datafrane that contains the minimum DI value is: ')
min_val.printSchema()
min_val.show()

print('The datafrane that contains the maximum DI value is: ')
max_val.printSchema()
max_val.show()
