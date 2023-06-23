# Databricks notebook source
# MAGIC %md
# MAGIC # Basic Databricks + Redis integration
# MAGIC
# MAGIC This notebook loads data from build-in Databricks samples as a dataframe and then writes it into Redis database. In Redis RediSearch indices are created and queries from Python are run.

# COMMAND ----------

#install Redis Python library
%pip install -q redis

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')")
df=spark.table("default.people10m")
df=df.limit(10000) #trim dataframe to fit results into 30MB Redis database
display(df)
#df.count()

# COMMAND ----------

from pyspark.sql.types import MapType, StringType	
from pyspark.sql.functions import *	
from pyspark.sql.types import StructType,StructField, StringType

#If range search on Date field is required it has to be converted to timestamp
df=df.withColumn("dobTimeStamp", col("birthDate").cast("integer"))
display(df)

# COMMAND ----------

import os
#REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
#REDIS_PORT = os.getenv("REDIS_PORT", "6379")
#REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
#Replace values below with your own if using Redis Cloud instance
REDIS_HOST="redis-17231.c228.us-central1-1.gce.cloud.redislabs.com"
REDIS_PORT=17231
REDIS_PASSWORD="0XKOePIFBCtuNvV6PhsXl3ysQYXXXXXX"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize Dataframe to Redis
# MAGIC
# MAGIC You compute cluster must include `com.redislabs:spark-redis_2.12:2.4.2` maven library. After adding library to the compute cluster - restart the cluster. 
# MAGIC
# MAGIC The code in a cell below should be suficcient to load dataframe into Redis.

# COMMAND ----------

df.write.format("org.apache.spark.sql.redis") \
      .mode("overwrite") \
      .option("table", "people") \
      .option("key.column", "id") \
      .option("host", REDIS_HOST) \
      .option("port", REDIS_PORT) \
      .option("auth", REDIS_PASSWORD) \
      .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Redis-py library
# MAGIC
# MAGIC In the following cells we'll use redis-py library to build a RediSearch index for the data in Redis and query it. Usually this code belongs to the application running outside of Databricks, but here we are doing it in the notebook just for the illustration purposes.

# COMMAND ----------

import redis
r = redis.Redis(
  host=REDIS_HOST,
  port=REDIS_PORT,
  password=REDIS_PASSWORD)

r.ping() #Basic Ping/Pong test. If you received "True" as a responce

r.hgetall("people:3766824") #retreive record by the known key

# COMMAND ----------

from redis.commands.search.field import (
    NumericField,
    TagField,
    TextField,
    GeoField,
    VectorField
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search import reducers
schema = (
    TextField("firstName", as_name="firstName"),
    TextField("lastName", as_name="lastName"),
    TextField("middleName", as_name="middleName"),
    TagField("ssn", as_name="ssn"),
    TagField("gender", as_name="gender"),
    NumericField("salary", as_name="salary"),
    NumericField("dobTimeStamp", as_name="dob")
    )
try:
    r.ft("idx:people").dropindex() # drop index if already exists
except redis.ResponseError:
    print("no existing index found")

r.ft("idx:people").create_index(schema,
                    definition=IndexDefinition(prefix=["people:"],
                    index_type=IndexType.HASH)
                    )

# COMMAND ----------

import pandas as pd
#helper function to display results of redis.ft().search() as a dataframe
def display_ft(res):
  if res.total==0:
    print("No matches found")
  else:
    res_df = pd.DataFrame([t.__dict__ for t in res.docs ]).drop(columns=["payload"])
    display(res_df)

# COMMAND ----------

query=Query("Ma*")
res=r.ft("idx:people").search(query)
display_ft(res)

# COMMAND ----------

query=Query("@lastName:Me* @dob:[315532800, inf]") #only lastName strts with Me and DOB is 1980 or later
res=r.ft("idx:people").search(query)
display_ft(res)
