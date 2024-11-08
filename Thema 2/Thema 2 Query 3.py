# Databricks notebook source
#Importing everything required for spark sql

import pyspark
from pyspark.sql import Window
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

#Query that create structs with Open,Date values for each entry in the original dataframe 
#Then finds the maximum struct by Open 
#And then returns a dataframe that has the maximum Open value and the Day that it was achieved

df_agn_open = df_agn.withColumn('Open_Date_struct', struct(df_agn.Open, df_agn.Date))
max_df_open = df_agn_open.agg(max('Open_Date_struct').alias('Max_Open'))
q_agn_open = max_df_open.withColumn('Date', max_df_open.Max_Open.Date).withColumn('Open', max_df_open.Max_Open.Open).drop('Max_Open')

df_ainv_open = df_ainv.withColumn('Open_Date_struct', struct(df_ainv.Open, df_ainv.Date))
max_df_open = df_ainv_open.agg(max('Open_Date_struct').alias('Max_Open'))
q_ainv_open = max_df_open.withColumn('Date', max_df_open.Max_Open.Date).withColumn('Open', max_df_open.Max_Open.Open).drop('Max_Open')

df_ale_open = df_ale.withColumn('Open_Date_struct', struct(df_ale.Open, df_ale.Date))
max_df_open = df_ale_open.agg(max('Open_Date_struct').alias('Max_Open'))
q_ale_open = max_df_open.withColumn('Date', max_df_open.Max_Open.Date).withColumn('Open', max_df_open.Max_Open.Open).drop('Max_Open')

# COMMAND ----------

#Query that create structs with Volume,Date values for each entry in the original dataframe
#Then finds the maximum struct by Volume
#And then returns a dataframe that has the maximum Volume value and the Day that it was achieved

df_agn_volume = df_agn.withColumn('Volume_struct', struct(df_agn.Volume, df_agn.Date))
max_df_volume = df_agn_volume.agg(max('Volume_struct').alias('Max_Volume'))
q_agn_volume = max_df_volume.withColumn('Date', max_df_volume.Max_Volume.Date).withColumn('Volume', max_df_volume.Max_Volume.Volume).drop('Max_Volume')

df_ainv_volume = df_ainv.withColumn('Volume_struct', struct(df_ainv.Volume, df_ainv.Date))
max_df_volume = df_ainv_volume.agg(max('Volume_struct').alias('Max_Volume'))
q_ainv_volume = max_df_volume.withColumn('Date', max_df_volume.Max_Volume.Date).withColumn('Volume', max_df_volume.Max_Volume.Volume).drop('Max_Volume')

df_ale_volume = df_ale.withColumn('Volume_struct', struct(df_ale.Volume, df_ale.Date))
max_df_volume = df_ale_volume.agg(max('Volume_struct').alias('Max_Volume'))
q_ale_volume = max_df_volume.withColumn('Date', max_df_volume.Max_Volume.Date).withColumn('Volume', max_df_volume.Max_Volume.Volume).drop('Max_Volume')

# COMMAND ----------

#Print the schema for each dataframe and show it, where each dataframe contains the maximum value of Open or Volume for each stock

print("Open values for AGN: ")
q_agn_open.printSchema()
q_agn_open.show()

print("Volume Values for AGN: ")
q_agn_volume.printSchema()
q_agn_volume.show()

print("Open values for AINV: ")
q_ainv_open.printSchema()
q_ainv_open.show()

print("Volume Values for AINV: ")
q_ainv_volume.printSchema()
q_ainv_volume.show()

print("Open values for ALE: ")
q_ale_open.printSchema()
q_ale_open.show()

print("Volume Values for ALE: ")
q_ale_volume.printSchema()
q_ale_volume.show()

