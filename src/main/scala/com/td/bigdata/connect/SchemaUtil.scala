package com.td.bigdata.connect

import com.sun.tools.javac.comp.Todo
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types._

/**
  * Created by zhengqh on 16/11/8.
  */
object SchemaUtil extends App{

  def row2df(sparkContext: SparkContext, sqlContext: SQLContext,
             rowRDD:RDD[Row],
             attributeTypes: List[(String,String)]): DataFrame ={
    val schema = parseSchema(attributeTypes)
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.printSchema()
    df
  }

  def parseSchema(attributeTypes: List[(String,String)]): StructType ={
    val fileds = attributeTypes.map(attType => {
      val attName = attType._1
      val typeName = attType._2
      val _type = typeName match {
        case "string" => StringType
        case "long" => LongType
        case "int" => IntegerType
        case _ => StringType
      }
      StructField(attName, _type, true)
    })
    val schema = StructType(fileds)
    schema
  }

}
