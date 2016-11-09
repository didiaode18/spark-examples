package com.td.bigdata.connect

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
  * Created by zhengqh on 16/11/7.
  */
object MockData {

  val localFile = "file:///Users/zhengqh/Github/spark-examples"

  def users(sqlContext: SQLContext): DataFrame ={
    val usersDf = sqlContext.read.json(s"$localFile/src/main/resources/users.json")
    usersDf
  }
  def users2(sqlContext: SQLContext): DataFrame ={
    val usersDf = sqlContext.read.json(s"$localFile/src/main/resources/users2.json")
    usersDf
  }

  def people(sparkContext: SparkContext): RDD[String] ={
    val people = sparkContext.textFile(s"$localFile/src/main/resources/people.txt")
    people
  }

  def peopleRDDRow(sparkContext: SparkContext): RDD[Row] ={
    val rowRDD = people(sparkContext).map(_.split(",")).map(p => Row(p(0), p(1).trim.toInt))
    rowRDD
  }

  case class Person(name: String, age: Int)

  def people2PersonDF(sparkContext: SparkContext, sqlContext: SQLContext): DataFrame ={
    val personRDD = people(sparkContext).map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    import sqlContext._
    import sqlContext.implicits._

    val df = personRDD.toDF()
    df
  }

  def registerTempTableAndShow(sqlContext: SQLContext, df: DataFrame, tbl: String): Unit ={
    df.registerTempTable(tbl)
    val results = sqlContext.sql(s"SELECT * FROM $tbl")
    results.show()
  }


}
