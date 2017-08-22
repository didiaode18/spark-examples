package example

import org.apache.hadoop.hbase.client.{HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, HTableDescriptor, TableName}
import org.apache.spark._

import scala.collection.JavaConverters._

object TestHBaseInput {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, args(0))
    conf.set(TableInputFormat.INPUT_TABLE, args(1))

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(args(1))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(1)))
      admin.createTable(tableDesc)
    }

    // build scanner with start and end row key
    val scanner = new Scan
    scanner.setReversed(true)
    val start = args(2) + "_" + args(3)
    val stop = args(2) + "_" + args(4)
    scanner.setStartRow(Bytes.toBytes(start))
    scanner.setStopRow(Bytes.toBytes(stop))

    def convertScanToString(scan: Scan): String = {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan);
      return Base64.encodeBytes(proto.toByteArray());
    }

    conf.set(TableInputFormat.SCAN, convertScanToString(scanner))

    // read hbase
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // process rdd
    val keyValue = hBaseRDD.map(x => x._2).map(x => x.getColumn(Bytes.toBytes("identity"), Bytes.toBytes("id")))

    val outPut = keyValue.flatMap { x =>
      x.asScala.map { cell =>
        Bytes.toString(CellUtil.cloneValue(cell))
      }
    }

    outPut.foreach(println)

    sc.stop()
  }
}