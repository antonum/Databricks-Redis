# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets/learning-spark-v2/people/'))

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')")
df=spark.table("default.people10m")
df=df.head(3000) #trim dataframe to fit results into 30MB Redis database
display(df)
#df.count()
