package com.github.crossdata

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 16/11/7.
  */
object CrossDataDemo1 extends App{

  import org.apache.spark.sql.crossdata._

  val sparkConf = new SparkConf().setAppName("CrossDataDemo1").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val xdContext = new XDContext(sqlContext.sparkContext)
  xdContext.sql("SHOW TABLES").show(false)

  /**
    16/11/07 15:26:03 WARN XDContext: User resource (core-application.conf) hasn't been found
    16/11/07 15:26:03 WARN XDContext: User file (core-application.conf) hasn't been found in classpath
    16/11/07 15:26:03 WARN XDContext: External file (/etc/sds/crossdata/server/core-application.conf) hasn't been found
    16/11/07 15:26:06 WARN CatalogUtils: There is no configured streaming catalog
    +---------+-----------+
    |tableName|isTemporary|
    +---------+-----------+
    +---------+-----------+
   */

  xdContext.sql("IMPORT TABLES USING com.stratio.crossdata.connector.cassandra OPTIONS (cluster 'Test Cluster', spark_cassandra_connection_host '127.0.0.1')").show(false)

  /**
    * make sure elasticsearch-hadoop or elasticsearch-spark only one need, and
    * also exclude elasticsearch-spark from crossdata-elasticsearch, cause it also conflict with elasticsearch-hadoop
    *
    16/11/07 15:41:04 INFO Version: Elasticsearch Hadoop v2.4.0 [4ab7a3ea4c]
    16/11/07 15:41:08 INFO Cluster: New Cassandra host /127.0.0.1:9042 added
    16/11/07 15:41:08 INFO CassandraConnector: Connected to Cassandra cluster: Test Cluster
    16/11/07 15:41:08 INFO DerbyCatalog: PersistentCatalog: Looking up table forseti.velocity_app
    16/11/07 15:41:08 INFO ImportTablesUsingWithOptions: Importing table forseti.velocity_app
    +-----------------------+-------+
    |tableIdentifier        |ignored|
    +-----------------------+-------+
    |[forseti, velocity_app]|false  |
    |[forseti, mytab]       |false  |
    |[demo, test]           |false  |
    |[demo, tweets]         |false  |
    +-----------------------+-------+
    */

  xdContext.sql("SHOW TABLES").show(false)
  /*
   +--------------------+-----------+
   |tableName           |isTemporary|
   +--------------------+-----------+
   |demo.test           |false      |
   |demo.tweets         |false      |
   |forseti.mytab       |false      |
   |forseti.velocity_app|false      |
   +--------------------+-----------+
   */

  //xdContext.sql("CREATE EXTERNAL TABLE demo.shopping_centers (name STRING, address STRING, employees INT, opening_year INT) USING cassandra OPTIONS (keyspace 'demo', cluster 'Test Cluster', pushdown 'true', spark_cassandra_connection_host '127.0.0.1', primary_key_string 'name')").show(false)

  //com.datastax.driver.core.DefaultResultSetFuture cannot be cast to shade.com.datastax.spark.connector.google.common.util.concurrent.ListenableFuture
  xdContext.sql("INSERT INTO demo.shopping_centers('name', 'address', 'employees', 'opening_year') VALUES " +
    "('Madrid', 'Avd. de Betanzos', 39, 2001), " +
    "('Sevilla', 'C/ Sierpes', 25, 2004), " +
    "('Valencia', 'C/ Mestalla', 33, 2001), " +
    "('Barcelona', 'Avd. Diagonal', 25, 2004), " +
    "('Bilbao', 'C/ Perez Galdos', 27, 2008)").show(false)

  /**
    * As this Job has 4 thread: setMaster("local[4]"), so there will be 4 task
    * Notice, each sql execution will gen one job, As we already execute 2 sql, so this is Job 3
    *
    16/11/07 16:29:36 INFO DAGScheduler: Got job 3 (runJob at RDDFunctions.scala:37) with 4 output partitions
    16/11/07 16:29:36 INFO DAGScheduler: Submitting 4 missing tasks from ResultStage 3 (MapPartitionsRDD[7] at sql at CrossDataDemo1.scala:67)
    16/11/07 16:29:36 INFO TaskSchedulerImpl: Adding task set 3.0 with 4 tasks
    16/11/07 16:29:36 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 3, localhost, partition 0,PROCESS_LOCAL, 2451 bytes)
    16/11/07 16:29:36 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 4, localhost, partition 1,PROCESS_LOCAL, 2446 bytes)
    16/11/07 16:29:36 INFO TaskSetManager: Starting task 2.0 in stage 3.0 (TID 5, localhost, partition 2,PROCESS_LOCAL, 2448 bytes)
    16/11/07 16:29:36 INFO TaskSetManager: Starting task 3.0 in stage 3.0 (TID 6, localhost, partition 3,PROCESS_LOCAL, 2534 bytes)
    16/11/07 16:29:36 INFO Executor: Running task 0.0 in stage 3.0 (TID 3)
    16/11/07 16:29:36 INFO Executor: Running task 1.0 in stage 3.0 (TID 4)
    16/11/07 16:29:36 INFO Executor: Running task 2.0 in stage 3.0 (TID 5)
    16/11/07 16:29:36 INFO Executor: Running task 3.0 in stage 3.0 (TID 6)
    16/11/07 16:29:36 INFO TableWriter: Wrote 1 rows to demo.shopping_centers in 0.268 s.
    16/11/07 16:29:36 INFO TableWriter: Wrote 2 rows to demo.shopping_centers in 0.268 s.  <<== We write 5 row, so this task take 2 row
    16/11/07 16:29:36 INFO TableWriter: Wrote 1 rows to demo.shopping_centers in 0.266 s.
    16/11/07 16:29:36 INFO TableWriter: Wrote 1 rows to demo.shopping_centers in 0.268 s.
    +--------------------+
    |Number of insertions|
    +--------------------+
    |5                   |
    +--------------------+
    */

  //xdContext.sql("CREATE EXTERNAL TABLE demo.sales (center STRING, products INT, total INT) USING cassandra OPTIONS (keyspace 'demo', cluster 'Test Cluster', pushdown 'true', spark_cassandra_connection_host '127.0.0.1', primary_key_string 'center')").show(false)
  xdContext.sql("INSERT INTO demo.sales('center', 'products', 'total') VALUES " +
    "('Bilbao', 25, 2500), " +
    "('Madrid', 51, 7700), " +
    "('Valencia', 23, 3300), " +
    "('Barcelona', 47, 6400), " +
    "('Sevilla', 28, 3200)").show(false)  //Job 7 Number of insertions:5
  xdContext.sql("SHOW TABLES").show(false)

  /**
    * now you should see this two table in keyspace demo, you can check cassandra by cqlsh to validate if create or not
    *
    +---------------------+-----------+
    |tableName            |isTemporary|
    +---------------------+-----------+
    |demo.sales           |false      |
    |demo.shopping_centers|false      |
    +---------------------+-----------+
    */

  xdContext.sql("SELECT * FROM demo.sales WHERE center = 'Madrid'").show(false)
  /**
    * Direct Read
    *
    16/11/07 16:29:40 INFO CassandraXDSourceRelation: filters: EqualTo(center,Madrid)
    16/11/07 16:29:40 INFO CassandraXDSourceRelation: Input Predicates: [EqualTo(center,Madrid)]
    16/11/07 16:29:40 INFO CassandraXDSourceRelation: pushdown filters: [Lorg.apache.spark.sql.sources.Filter;@270f7b4d

    +------+--------+-----+
    |center|products|total|
    +------+--------+-----+
    |Madrid|51      |7700 |
    +------+--------+-----+
    */

  xdContext.sql("SELECT * FROM demo.sales INNER JOIN demo.shopping_centers ON center = name ORDER BY total").show(false)
  /**
    * Spark Read
    *
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: filters:
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: Input Predicates: []
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: pushdown filters: [Lorg.apache.spark.sql.sources.Filter;@39685204
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: filters:
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: Input Predicates: []
    16/11/07 16:29:42 INFO CassandraXDSourceRelation: pushdown filters: [Lorg.apache.spark.sql.sources.Filter;@43df1377
    16/11/07 16:29:43 INFO SparkContext: Starting job: show at CrossDataDemo1.scala:84
    16/11/07 16:29:43 INFO DAGScheduler: Registering RDD 36 (show at CrossDataDemo1.scala:84)
    16/11/07 16:29:43 INFO DAGScheduler: Registering RDD 32 (show at CrossDataDemo1.scala:84)
    16/11/07 16:29:43 INFO DAGScheduler: Got job 10 (show at CrossDataDemo1.scala:84) with 200 output partitions
    16/11/07 16:29:48 INFO DAGScheduler: Job 10 finished: show at CrossDataDemo1.scala:84, took 5.111240 s
    +---------+--------+-----+---------+----------------+---------+------------+
    |center   |products|total|name     |address         |employees|opening_year|
    +---------+--------+-----+---------+----------------+---------+------------+
    |Bilbao   |25      |2500 |Bilbao   |C/ Perez Galdos |27       |2008        |
    |Sevilla  |28      |3200 |Sevilla  |C/ Sierpes      |25       |2004        |
    |Valencia |23      |3300 |Valencia |C/ Mestalla     |33       |2001        |
    |Barcelona|47      |6400 |Barcelona|Avd. Diagonal   |25       |2004        |
    |Madrid   |51      |7700 |Madrid   |Avd. de Betanzos|39       |2001        |
    +---------+--------+-----+---------+----------------+---------+------------+
    */
}
