# Databricks notebook source
# DBTITLE 1,Import library
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,create dataframe
df_google_app_raw = spark.read.load('/FileStore/tables/googleplaystore.csv',format='csv',sep=',',header='true',escape='"',inferschema='true') 

# COMMAND ----------

df_google_app_raw.count()

# COMMAND ----------

df_google_app_raw.show(2)

# COMMAND ----------

# DBTITLE 1,check schema
df_google_app_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Data cleaning Steps 

# COMMAND ----------

# DBTITLE 1,Drop extra columns which is not required for our Analysis and storing in a new dataFrame  
df_google_app_1 = df_google_app_raw.drop("Size","Content Rating","Last Updated","Current Ver","Android Ver")

# COMMAND ----------

# DBTITLE 1,Validate all the required column and how values are stored
df_google_app_1.show(2) 

# COMMAND ----------

# DBTITLE 1,Check Data type of every column
df_google_app_1.printSchema()

# COMMAND ----------

# DBTITLE 1,Changing Datatype of columns and removing extra symbols/characters (+,$) from the value and storing in a new DataFrame
df_google_app_2 = df_google_app_1.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
    .withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
    .withColumn("Installs",col("Installs").cast(IntegerType()))\
    .withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
    .withColumn("Price",col("Price").cast(IntegerType()))


# COMMAND ----------

# DBTITLE 1,Validate all the Datatypes of every columns.
df_google_app_2.printSchema()

# COMMAND ----------

# DBTITLE 1,Validate values are correctly showing in column, extra symbols/special characters (+,$)are removed or not
df_google_app_2.show(10)

# COMMAND ----------

# DBTITLE 1,Creating views of our final DataFrame, using this view we can perform SQL operation ()sql query
df_google_app_2.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apps

# COMMAND ----------

# DBTITLE 1,1. Top 10 reviews given to the app
# MAGIC %sql
# MAGIC select  App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------

# DBTITLE 1,2. Top 10 installs apps and distribution of type (Free/paid)
# MAGIC %sql
# MAGIC select  App,Type, sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,3. Category wise distribution of installed app
# MAGIC %sql
# MAGIC select   Category,sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC --LIMIT 10

# COMMAND ----------

# DBTITLE 1,3.2 . Which Category's app installed most name of the app which is under this most popular category
# MAGIC %sql
# MAGIC select  App, Category,sum(Installs) from apps
# MAGIC group by 1,2
# MAGIC order by 3 desc
# MAGIC --LIMIT 10

# COMMAND ----------

# DBTITLE 1,4. Top paid app
# MAGIC %sql
# MAGIC select App,sum(Price) from apps
# MAGIC where Type='Paid'
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,4.2 Most installed Paid App
# MAGIC %sql
# MAGIC select * from apps
# MAGIC where Type="Paid"
# MAGIC order by Installs desc

# COMMAND ----------

# DBTITLE 1,5. Top paid rating app
# MAGIC %sql
# MAGIC Select App,sum (Rating) from apps
# MAGIC where Type="Paid"
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------


