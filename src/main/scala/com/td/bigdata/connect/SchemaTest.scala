package com.td.bigdata.connect

import com.td.bigdata.connect.MockData.Person
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

/**
  * Created by zhengqh on 16/11/7.
  */
object SchemaTest extends App{

  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext._
  import sqlContext.implicits._

  //打印Schema信息
  def printSchemaMetadata(): Unit ={
    val usersDf = MockData.users(sqlContext)
    usersDf.printSchema()
    /**
    root
     |-- city: string (nullable = true)
     |-- country: string (nullable = true)
     |-- email: string (nullable = true)
     |-- id: long (nullable = true)
     |-- ip: string (nullable = true)
     |-- name: string (nullable = true)
      */

    usersDf.schema.fields.foreach(e=> println(""+e.name + ":" + e.dataType))
    /**
    city:StringType
    country:StringType
    email:StringType
    id:LongType
    ip:StringType
    name:StringType
      */
  }

  //手动解析Schema,将RDD生成DF
  def parseSchemaTest(): Unit ={
    //RDD[Row]中Row的类型必须和schema一致,比如Row(String,Integer)对应List(("name",""),("age","int"))
    //如果是List(("name",""),("age","string"))就会报错,因为Row的第二个字段是integer,而schema=string,冲突
    val peopleRDDRow = MockData.peopleRDDRow(sparkContext)
    val peopleDF = SchemaUtil.row2df(sparkContext, sqlContext, peopleRDDRow, List(("name",""),("age","int")))
    peopleDF.show()
  }
  //parseSchemaTest()

  //使用case class映射生成DF
  def caseClassDf(): Unit ={
    val peopleRDD = MockData.people(sparkContext)
    val peopleDF = peopleRDD.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

    peopleDF.printSchema()
    peopleDF.show()

    val peopleDF2 = MockData.people2PersonDF(sparkContext, sqlContext)
    peopleDF2.printSchema()
    peopleDF2.show()
  }
  caseClassDf()

}
