package com.td.bigdata.connect

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhengqh on 16/11/7.
  *

  Append
    [Eláteia,Greece,bsmithrl@simplemachines.org,994,9.19.204.44,Bety]
    [Shangjing,China,alewisrm@canalblog.com,995,14.207.119.126,Ana]
    [Eláteia,Greece,bsmithrl@simplemachines.org,994,9.19.204.44,Betty]
    [Shangjing,China,alewisrm@canalblog.com,995,14.207.119.126,Anna]
    [Tsarychanka,Ukraine,dgarrettrn@japanpost.jp,996,111.252.63.159,David]
    [Koilás,Greece,hgilbertro@skype.com,997,29.57.181.250,Heather]
    [Tsarychanka,Ukraine,dgarrettrn@japanpost.com,996,111.252.63.159,David]
    [Koran,Greece,carlis@google.com,1001,29.57.181.250,Calis]
    [Mapiripán,Colombia,ddanielsrp@statcounter.com,998,19.205.181.99,Diane]
    [El Cairo,Colombia,pfullerrq@reuters.com,999,210.248.121.194,Philip]
    [Karabash,Russia,mfordrr@shop-pro.jp,1000,224.21.41.52,Maria]

  Overwrite
    [Eláteia,Greece,bsmithrl@simplemachines.org,994,9.19.204.44,Bety]
    [Shangjing,China,alewisrm@canalblog.com,995,14.207.119.126,Ana]
    [Tsarychanka,Ukraine,dgarrettrn@japanpost.com,996,111.252.63.159,David]
    [Koran,Greece,carlis@google.com,1001,29.57.181.250,Calis]

  */
object HDFSWriteRead extends App {

  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  def hdfs(path: String, mode: SaveMode = SaveMode.Append): Unit ={
    val usersDf = MockData.users(sqlContext)
    usersDf.write.mode(mode).save(path)

    val usersDf2 = MockData.users2(sqlContext)
    usersDf2.write.mode(mode).save(path)

    //read parquet file
    val df = sqlContext.read.parquet(path)
    df.collect().foreach(println)
  }

  hdfs("users1", SaveMode.Append)     //append:追加, 更新记录不会覆盖已有的记录
  hdfs("users2", SaveMode.Overwrite)  //overwrite:先删除,再添加. 而不是真正的覆盖/更新
}
