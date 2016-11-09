package com.td.bigdata.connect

import java.util.Properties

import com.td.bigdata.connect.MockData.Person2
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 16/11/7.
  *

  * MySQL schema
  * CREATE TABLE `users2` (
  * `id` int(11) DEFAULT NULL,
  * `name` varchar(50) DEFAULT NULL,
  * `email` varchar(50) DEFAULT NULL,
  * `city` varchar(50) DEFAULT NULL,
  * `country` varchar(50) DEFAULT NULL,
  * `ip` varchar(50) DEFAULT NULL
  * );

  CREATE TABLE `person` (
      `id` int(11) DEFAULT NULL,
      `ts` int(11) DEFAULT NULL,
      `name` varchar(50) DEFAULT NULL
    );

  insert into person(id,ts,name)values(1,1,'Mick');
  insert into person(id,ts,name)values(2,2,'Mick');
  insert into person(id,ts,name)values(3,3,'Mick');
  */
object MySQLIncrement extends App {

  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

  val props = new Properties()
  props.put("user", "root")
  props.put("password", "root")
  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8"


  def incrementReadMySQL(): Unit ={

    //模拟MySQL数据库写入
    val p1 = sparkContext.parallelize(List(
      Person2(1,1L,"Mick"), Person2(2,2L,"John"), Person2(3,3L,"Tick")
    )).toDF()
    p1.write.mode(SaveMode.Append).jdbc(url, "person", props)

    //JOB-1
    //第一次读取MySQL,写入HDFS
    val df1 = sqlContext.read.jdbc(url, "(select * from person) as t1", props)
    df1.write.mode(SaveMode.Append).save("person")
    var last = lastValue(df1)

    //模拟MySQL更新数据库, 实际中更新是会替换已有记录,这里用Append,以及最新的ts
    val p2 = sparkContext.parallelize(List(
      Person2(1,4L,"Mickey"), Person2(3,4L,"Teck"), Person2(4,5L,"Mike")
    )).toDF()
    p2.write.mode(SaveMode.Append).jdbc(url, "person", props)

    //JOB-2
    //第二次读取MySQL,写入HDFS
    val df2 = sqlContext.read.jdbc(url, s"(select * from person where ts>$last) as t1", props)
    df1.write.mode(SaveMode.Append).save("person")
    last = lastValue(df1)

    //Merge HDFS
    val mergeDf = sqlContext.read.parquet("person")
    mergeDf.collect().foreach(println)

    val resultDf = mergeDf.groupBy("id").max("ts")
    resultDf.collect().foreach(println)
    resultDf.write.mode(SaveMode.Overwrite).save("person")
  }

  def lastValue(df: DataFrame) = {
    val rdd = df.select("ts").rdd
    rdd.map(r=>r.getAs[Long]("ts")).reduce((a, b) => Math.max(a, b))
  }

  def mergeTest(): Unit ={
    val p1 = sparkContext.parallelize(List(
      Person2(1,1L,"Mick"), Person2(2,2L,"John"), Person2(3,3L,"Tick"),
      Person2(1,4L,"Mickey"), Person2(3,4L,"Teck"), Person2(4,5L,"Mike")
    )).toDF()

    val resultDf = p1.groupBy("id").max("ts")
    resultDf.collect().foreach(println)
  }

  def findMax(): Unit ={
    val p1 = sparkContext.parallelize(List(
      Person2(1,1L,"Mick"), Person2(2,2L,"John"), Person2(3,3L,"Tick"),
      Person2(1,4L,"Mickey"), Person2(3,4L,"Teck"), Person2(4,5L,"Mike")
    )).toDF()

    val lastValue = p1.select("ts").rdd.map(r=>r.getAs[Long]("ts")).reduce((a, b) => Math.max(a, b))
    println(lastValue)
  }
  findMax()
}
