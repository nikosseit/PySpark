# Databricks notebook source
#Importing everything required for spark sql

import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

#Making the dataframes from the tables that were created from each file

df_agn = spark.read.option("header",True).csv("/FileStore/tables/agn_us.txt")
df_ainv = spark.read.option("header",True).csv("/FileStore/tables/ainv_us.txt")
df_ale = spark.read.option("header",True).csv("/FileStore/tables/ale_us.txt")

# COMMAND ----------

#Transform column date from String type to Date type

df_agn = df_agn.withColumn('Date',to_date('Date'))
df_ainv = df_ainv.withColumn('Date',to_date('Date'))
df_ale = df_ale.withColumn('Date',to_date('Date'))

# COMMAND ----------

#Transforming each column that has double type values from String type to Double type

columns = ['Open','High','Low','Close']
for x in columns:
    df_agn = df_agn.withColumn(x,col(x).cast('double'))
    df_ainv = df_ainv.withColumn(x,col(x).cast('double'))
    df_ale = df_ale.withColumn(x,col(x).cast('double'))

# COMMAND ----------

#Transform each column that has integer type values from String type to Int type

columns = ['Volume','OpenInt']
for x in columns:
    df_agn = df_agn.withColumn(x,col(x).cast('int'))
    df_ainv = df_ainv.withColumn(x,col(x).cast('int'))
    df_ale = df_ale.withColumn(x,col(x).cast('int'))

# COMMAND ----------

#Query that returns a dataframe where the Open value for each entry is over 35 dollars and then counts how many entries are in each dataframe

q_agn = df_agn.filter(df_agn["Open"] >= 35)
agn_count = q_agn.count()

q_ainv = df_ainv.filter(df_ainv["Open"] >= 35)
ainv_count = q_ainv.count()

q_ale = df_ale.filter(df_ale["Open"] >= 35)
ale_count = q_ale.count()

# COMMAND ----------

#Print the schema for each dataframe, show the first 5 entries and print how many days the open price was over 35 dollars

print("Values for AGN:")
q_agn.printSchema()
q_agn.show(5)

print("Values for AINV:")
q_ainv.printSchema()
q_ainv.show(5)

print("Values for ALE:")
q_ale.printSchema()
q_ale.show(5)

print("Number of days that AGN stock opened at a price over 35 dollars: ",agn_count,"\n")
print("Number of days that AINV stock opened at a price over 35 dollars: ",ainv_count,"\n")
print("Number of days that ALE stock opened at a price over 35 dollars: ",ale_count,"\n")
