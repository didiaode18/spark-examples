package com.td.bigdata.spark.streaming

import java.sql.{PreparedStatement, Connection, DriverManager}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
  * Created by zhengqh on 16/11/9.
  */
object StreamingMySQL {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetCatWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val dstream = ssc.socketTextStream("localhost", 9999)

    dstream.foreachRDD(rdd => {
      def func(records: Iterator[String]) {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://localhost:3306/test";
          val user = "root";
          val password = "root"
          conn = DriverManager.getConnection(url, user, password)
          records.flatMap(_.split(" ")).foreach(word => {
            val sql = "insert into words(word) values (?)";
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, word)
            stmt.executeUpdate();
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }

      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
