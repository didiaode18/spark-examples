package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase._

/**
  * Created by zhengqh on 17/3/15.
  */
object TestHBaseRDD {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark sql hbase test")
    val sc = new SparkContext(sparkConf)

    implicit val config = HBaseConfig(
      "hbase.rootdir" -> "/hbase",
      "hbase.zookeeper.quorum" -> "10.57.17.126,10.57.17.125,10.57.17.80"
    )

    val table = "test"
    val r1 = ("row6", Map("a" -> "a"))
    val r2 = ("row7", Map( "b" -> "b", "c" -> "c"))

    // 针对单一列族(CF), RDD类型为: RDD[(K, Map[Q, V])],  K:主键, Q:列, V:列的值
    // rdd to hbase
    val rddWrite: RDD[(String, Map[String, String])] = sc.parallelize(Array(r1, r2))
    rddWrite.toHBase(table, "cf")

    // use hbase api to create table
    val table2 = "test2"
    val admin = Admin()
    val tableExist = admin.tableExists(table2, "cf")
    if(!tableExist) {
      admin.createTable(table2, "cf")
    }

    // rdd to hbase again
    val map = Map("a" -> "a", "b" -> 2)
    val r3 = ("row1", map)
    val rdd2: RDD[(String, Map[String, String])] = sc.parallelize(Array(r3))
    rdd2.toHBase(table2, "cf")

    // read hbase to rdd
    val columns = Map("cf" -> Set("a", "b", "c"))
    val rddRead = sc.hbase[String](table, columns)
    rddRead.collect().foreach(println)
  }

}
