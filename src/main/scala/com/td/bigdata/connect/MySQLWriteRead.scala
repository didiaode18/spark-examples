package com.td.bigdata.connect

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 16/11/7.
  *

  DataFrame schema
    root
     |-- city: string (nullable = true)
     |-- country: string (nullable = true)
     |-- email: string (nullable = true)
     |-- id: long (nullable = true)
     |-- ip: string (nullable = true)
     |-- name: string (nullable = true)

  MySQL schema
    CREATE TABLE `users2` (
      `id` int(11) DEFAULT NULL,
      `name` varchar(50) DEFAULT NULL,
      `email` varchar(50) DEFAULT NULL,
      `city` varchar(50) DEFAULT NULL,
      `country` varchar(50) DEFAULT NULL,
      `ip` varchar(50) DEFAULT NULL
    );
    mysql> select * from users;
    +------+---------+-----------------------------+-------------+----------+-----------------+
    | id   | name    | email                       | city        | country  | ip              |
    +------+---------+-----------------------------+-------------+----------+-----------------+
    |  994 | Betty   | bsmithrl@simplemachines.org | Eláteia     | Greece   | 9.19.204.44     |
    |  998 | Diane   | ddanielsrp@statcounter.com  | Mapiripán   | Colombia | 19.205.181.99   |
    |  995 | Anna    | alewisrm@canalblog.com      | Shangjing   | China    | 14.207.119.126  |
    |  999 | Philip  | pfullerrq@reuters.com       | El Cairo    | Colombia | 210.248.121.194 |
    |  996 | David   | dgarrettrn@japanpost.jp     | Tsarychanka | Ukraine  | 111.252.63.159  |
    | 1000 | Maria   | mfordrr@shop-pro.jp         | Karabash    | Russia   | 224.21.41.52    |
    |  997 | Heather | hgilbertro@skype.com        | Koilás      | Greece   | 29.57.181.250   |
    +------+---------+-----------------------------+-------------+----------+-----------------+

    mysql> select * from users1;
    +-------------+----------+-----------------------------+------+-----------------+---------+
    | city        | country  | email                       | id   | ip              | name    |
    +-------------+----------+-----------------------------+------+-----------------+---------+
    | Eláteia     | Greece   | bsmithrl@simplemachines.org |  994 | 9.19.204.44     | Betty   |
    | Mapiripán   | Colombia | ddanielsrp@statcounter.com  |  998 | 19.205.181.99   | Diane   |
    | Shangjing   | China    | alewisrm@canalblog.com      |  995 | 14.207.119.126  | Anna    |
    | El Cairo    | Colombia | pfullerrq@reuters.com       |  999 | 210.248.121.194 | Philip  |
    | Karabash    | Russia   | mfordrr@shop-pro.jp         | 1000 | 224.21.41.52    | Maria   |
    | Tsarychanka | Ukraine  | dgarrettrn@japanpost.jp     |  996 | 111.252.63.159  | David   |
    | Koilás      | Greece   | hgilbertro@skype.com        |  997 | 29.57.181.250   | Heather |
    | Tsarychanka | Ukraine  | dgarrettrn@japanpost.com    |  996 | 111.252.63.159  | David   |
    | Eláteia     | Greece   | bsmithrl@simplemachines.org |  994 | 9.19.204.44     | Bety    |
    | Koran       | Greece   | carlis@google.com           | 1001 | 29.57.181.250   | Calis   |
    | Shangjing   | China    | alewisrm@canalblog.com      |  995 | 14.207.119.126  | Ana     |
    +-------------+----------+-----------------------------+------+-----------------+---------+
    7 rows in set (0.01 sec)

    mysql> select * from users2;
    +-------------+---------+-----------------------------+------+----------------+-------+
    | city        | country | email                       | id   | ip             | name  |
    +-------------+---------+-----------------------------+------+----------------+-------+
    | Eláteia     | Greece  | bsmithrl@simplemachines.org |  994 | 9.19.204.44    | Bety  |
    | Shangjing   | China   | alewisrm@canalblog.com      |  995 | 14.207.119.126 | Ana   |
    | Tsarychanka | Ukraine | dgarrettrn@japanpost.com    |  996 | 111.252.63.159 | David |
    | Koran       | Greece  | carlis@google.com           | 1001 | 29.57.181.250  | Calis |
    +-------------+---------+-----------------------------+------+----------------+-------+
    4 rows in set (0.00 sec)


  */
object MySQLWriteRead extends App {

  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val props = new Properties()
  props.put("user", "root")
  props.put("password", "root")
  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8"

  def readTest(table: String): Unit ={
    val jdbcDF = sqlContext.read.jdbc(url, table, props)
    jdbcDF.collect().foreach(println)
  }

  def jdbc(table: String, mode: SaveMode = SaveMode.Append): Unit ={
    val usersDf = MockData.users(sqlContext)
    usersDf.write.mode(mode).jdbc(url, table, props)

    val usersDf2 = MockData.users2(sqlContext)
    usersDf2.write.mode(mode).jdbc(url, table, props)

    readTest(table)
  }

  jdbc("users1", SaveMode.Append)     //append:追加, 更新记录不会覆盖已有的记录
  jdbc("users2", SaveMode.Overwrite)  //overwrite:先删除,再添加. 而不是真正的覆盖/更新
}
