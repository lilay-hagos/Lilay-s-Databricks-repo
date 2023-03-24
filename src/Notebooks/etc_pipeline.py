# Databricks notebook source
from pyspark.sql.functions import datediff
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
 


# COMMAND ----------

# MAGIC %md #### Read in the Dataset 

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ### we want to transform some data

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', datediff(current_date(), df_drivers.dob)/365)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_joined = df_drivers.select('driverId', 'driverRef', 'code', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on = ['driverID'])

# COMMAND ----------

display(df_joined)

# COMMAND ----------

df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header=True)

# COMMAND ----------

display(df_races)

# COMMAND ----------



# COMMAND ----------

df_join_races = df_joined.join(df_races.select('year', 'name', 'raceId'), on=['raceId'])

# COMMAND ----------

display(df_join_races)

# COMMAND ----------

df_join_races = df_join_races.drop('raceId', 'driverId')

# COMMAND ----------

display(df_join_races)

# COMMAND ----------

df_agg_age = df_join_races.groupby('age').agg(avg('milliseconds'))

# COMMAND ----------

df_agg_age.write.csv('s3://columbia-gr5069-main/processed/inclass/laptimes_by_age.csv')

# COMMAND ----------


