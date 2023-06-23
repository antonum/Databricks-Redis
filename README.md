# Databricks-Redis

Example notebook loads data from build-in Databricks samples as a dataframe and then writes it into Redis database. In Redis RediSearch indices are created and queries from Python are run.

You can use this example to materialize Databricks/Spark dataframes as Redis Hashes

## Add Spark-Redis JAR file to Cluster

To add required Redis-Spark libraries to your runtime add `com.redislabs:spark-redis_2.12:2.4.2` maven library to your Cluster Libraries section. You might need to restart runtime after library was added.

![alt text](media/spark-redis-jar.png)


## Add repo to the Workspace

In your Databricks Workspace Repos->Add Repo, enter `https://github.com/antonum/Databricks-Redis.git` as Git repository URL.

![alt text](media/add-repo.png)

## Get Free Redis Cloud account

https://redis.com/try-free/ - sign up with google account. Capture the URL, Port and default user password for the database.

Change the following lines in your notebook to use your own Redis Cloud endpoint and password:

```python
#Replace values below with your own if using Redis Cloud instance
REDIS_HOST="redis-17231.c228.us-central1-1.gce.cloud.redislabs.com"
REDIS_PORT=17231
REDIS_PASSWORD="0XKOePIFBCtuNvV6PhsXl3ysQYXXXXXX"
```

## References

- Spark-Redis https://github.com/RedisLabs/spark-redis
- Redis-py https://github.com/redis/redis-py 
- Redis Search commands: https://redis.io/commands/?group=search
- Redis Workshops https://github.com/antonum/Redis-Workshops (See `03-Advanced_RedisSearch` for advanced redis search examples)

## Saving Dataframe to Redis

The following code fragment would load content of Spark dataframe to Redis as Hash keys. Key names would be `"people:1234"` where people is `table` option and 123 is a value of `id` column from `key.column` option.

```python
df.write.format("org.apache.spark.sql.redis") \
      .mode("overwrite") \
      .option("table", "people") \
      .option("key.column", "id") \
      .option("host", REDIS_HOST) \
      .option("port", REDIS_PORT) \
      .option("auth", REDIS_PASSWORD) \
      .save()
```

For more information check Spark-Redis github and documentation https://github.com/RedisLabs/spark-redis