# spark-elasticsearch-connector

A library for querying Elasticsearch with Apache Spark

# Compatability

This library is compatible with Spark-2.4.x and Elasticsearch-6.x

# Usage

## Compile

```
sbt clean assembly
```

## Using with spark-shell

```
bin/spark-shell --jars spark-elasticsearch-connector-assembly-1.0.0-SNAPSHOT.jar --conf spark.sql.extensions=org.apache.spark.sql.rzlabs.ElasticExtensionsBuilder
```

In spark-shell, a temp table could be created like this:

```
val df = spark.read.format("org.rzlabs.elastic")
  .option("index", "news")
  .option("host": "localhost:9200")
  .load
df.createOrReplacTempView("news")
spark.sql("select title, publish_time from news where publish_time >= '2020-01-01T00:00L00' order by pblish_time DESC").show
```

or you can create a hive table:

```
spark.sql("""
  create table news using org.rzlabs.elastic options (
    index "news",
    host: "localhost:9200"
  )
""")
```

# Options

|option|required|default|description|
|-|-|-|-|
|host|No|localhost:9200|Elasticsearch server host with http port|
|index|Yes|null|Elaticsearch index name|
|type|No|The first type definition in the specific index mappings|the type name in a specific index|
