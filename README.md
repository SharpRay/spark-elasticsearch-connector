# spark-elasticsearch-connector

A library for querying Elasticsearch with Apache Spark

## Compatability

This library is compatible with Spark-2.4.x and Elasticsearch-6.x

## Usage

### Compile

```
sbt clean assembly
```

### Using with spark-shell

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

## Options

|option|required|default|description|
|-|-|-|-|
|host|No|localhost:9200|Elasticsearch server host with http port|
|index|Yes|null|Elaticsearch index name|
|type|No|The first type definition in the specific index mappings|the type name in a specific index|
|cacheIndexMappings|No|false|If re-pulling the index mappings or not when recreate the DF use the same options|
|skipUnknownTypeFields|No|false|If skip unknown type fields or not|
|debugTransformation|No|false|Log debug information about the transformations or not|
|timeZoneId|No|GMT|Time zone id will affect the Timestamp type field|
|dateTypeFormat|No|strict_date_optional_time\|\|epoch_millis|Date type format specified for `Date` type in Elasticsearch. For more information, please visit: https://www.elastic.co/guide/en/elasticsearch/reference/6.8/date.html|
|nullFillNonexistentFieldValue|No|false|If using null value to fill the nonexistent field or not, if true, then use null to fill, otherwise throw ElasticIndexException when a field is nonexistent in a InternalRow|
|defaultLimit|No|Int.MaxValue|The value of the setting `index.max_result_window` of Elasticsearch|

## Major features

### Currently

* Support fields projection and pruning.
* Support nested fields schema mapping (to StructField).
* Implement `MATCH` and `MATCH_PHRASE` predicates which will be pushed down as `match` and `match_phrase` queries.
* Implement `OFFSET` keyword parsing which will be pushed down as `from` parameter to support pagination.
* `Limit` operator will be pushed down as `size` parameter.
* `Offset` operator on top of Aggregate will be transformed to `CollectOffsetExec` node in planning phase to support global pagination.
* `Like` and `RLike` predicates will be pushed down as `wildcard` and `regexp` queries.
* `And` and `Or` predicates will be pushed down as `bool` query.
* `Sort`operator will be pushed down as `sort` parameter. 
* Support Join/Union operator. You can join or union es index with other datasources without any performance degradation.

### In the future

* Support `Aggregate` operator pushdown to support `agg` query in Elasticsearch to improve the aggregation performance.
* Support `Insert` operation to support writting data to Elasticsearch index.
* Support nested fields query.
* ...
