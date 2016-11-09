package com.td.bigdata.connect

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhengqh on 16/11/7.
  *
    +----+-----------+--------+--------------------+---------------+-------+
    |  id|       city| country|               email|             ip|   name|
    +----+-----------+--------+--------------------+---------------+-------+
    | 998|  Mapiripán|Colombia|ddanielsrp@statco...|  19.205.181.99|  Diane|
    | 994|    Eláteia|  Greece|bsmithrl@simplema...|    9.19.204.44|  Betty|
    | 997|     Koilás|  Greece|hgilbertro@skype.com|  29.57.181.250|Heather|
    | 996|Tsarychanka| Ukraine|dgarrettrn@japanp...| 111.252.63.159|  David|
    | 995|  Shangjing|   China|alewisrm@canalblo...| 14.207.119.126|   Anna|
    | 999|   El Cairo|Colombia|pfullerrq@reuters...|210.248.121.194| Philip|
    |1000|   Karabash|  Russia| mfordrr@shop-pro.jp|   224.21.41.52|  Maria|
    +----+-----------+--------+--------------------+---------------+-------+

    cqlsh:testkeyspace> select * from cassandradf ;

     id   | city        | country  | email                       | ip              | name
    ------+-------------+----------+-----------------------------+-----------------+---------
      998 |   Mapiripán | Colombia |  ddanielsrp@statcounter.com |   19.205.181.99 |   Diane
      994 |     Eláteia |   Greece | bsmithrl@simplemachines.org |     9.19.204.44 |   Betty
      997 |      Koilás |   Greece |        hgilbertro@skype.com |   29.57.181.250 | Heather
      996 | Tsarychanka |  Ukraine |     dgarrettrn@japanpost.jp |  111.252.63.159 |   David
      995 |   Shangjing |    China |      alewisrm@canalblog.com |  14.207.119.126 |    Anna
      999 |    El Cairo | Colombia |       pfullerrq@reuters.com | 210.248.121.194 |  Philip
     1000 |    Karabash |   Russia |         mfordrr@shop-pro.jp |    224.21.41.52 |   Maria

  */
object CassandraReadWrite extends App{

  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[2]")
    .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    .setIfMissing("spark.cassandra.connection.port", "9042")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  private def executeCommand(conn: CassandraConnector, command: String): Unit = {
    conn.withSessionDo(session => session.execute(command))
  }

  val cassandraFormat = "org.apache.spark.sql.cassandra"
  val cassandraKeyspace = "testkeyspace"
  val cassandraTable = "cassandradf"

  val connector = CassandraConnector(sparkContext.getConf)
  executeCommand(connector, s"CREATE KEYSPACE IF NOT EXISTS $cassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  executeCommand(connector, s"CREATE TABLE IF NOT EXISTS $cassandraKeyspace.$cassandraTable (id int PRIMARY KEY, name text, email text, city text, country text, ip text)")

  val usersDf = MockData.users(sqlContext)
  usersDf.printSchema()

  val cassandraOptions = Map("table" -> cassandraTable, "keyspace" -> cassandraKeyspace)

  //with DataFrame methods
  usersDf.write
    .format(cassandraFormat)
    .mode(Append)
    .options(cassandraOptions)
    .save()

  val cassandraDf = sqlContext.read.format(cassandraFormat)
    .options(cassandraOptions)
    .load()
    .select("*")
  cassandraDf.show()

}
