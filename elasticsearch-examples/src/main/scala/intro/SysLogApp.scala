package intro

import org.apache.hadoop.io.{Text, MapWritable, NullWritable}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.matching.Regex

/**
  * Created by zhengqh on 17/8/22.
  */
object SysLogApp {

  /**
  {
      "_index": "syslog",
      "_type": "entry",
      "_id": "ThOMFd8TSViYMtEOeq6eBw",
      "_score": 1,
      "_source": {
         "hostname": "oolong",
         "process": "pulseaudio",
         "timestamp": "Nov 12 14:34:15",
         "message": "[alsa-sink] alsa-sink.c: We were woken up with POLLOUT set -- however a subsequent snd_pcm_avail() returned 0 or another value < min_avail.",
         "pid": "2139"
      }
   }
    */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ESHadoopDemo").setMaster("local")
    conf.set("spark.es.index.auto.create", "true")
    conf.set("spark.es.nodes","localhost:9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val syslog = sc.textFile("/var/log/syslog")
    val re = """(\w{3}\s+\d{2} \d{2}:\d{2}:\d{2}) (\w+) (\S+)\[(\d+)\]: (.+)""".r

    val entries = syslog.collect {
      case re(timestamp, hostname, process, pid, message) =>
        Map("timestamp" -> timestamp, "hostname" -> hostname, "process" -> process, "pid" -> pid, "message" -> message)
    }
    // convert to Writables: RDD[MapWritable]
    val writables = entries.map(toWritable)
    // message the types so the collection is typed to an OutputFormat[Object, Object]
    val output = writables.map { v => (NullWritable.get.asInstanceOf[Object], v.asInstanceOf[Object]) }
    // index the data to elasticsearch
    sc.hadoopConfiguration.set("es.resource", "syslog/entry")
    //output.saveAsHadoopFile[EsOutputFormat]("-")

    //sample data
    val tweet = Map("user" -> "kimchy", "post_date" -> "2009-11-15T14:12:12", "message" -> "trying out Elastic Search")
    val tweets = sc.makeRDD(Seq(tweet))
    val samWritables = tweets.map(toWritable)
    val samOutput = samWritables.map { v => (NullWritable.get : Object, v : Object) }  //没有使用asInstanceOf,直接用:声明为Object
    samOutput.saveAsHadoopFile("")
  }

  // helper function to convert Map to a Writable
  def toWritable(in: Map[String, String]) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    m
  }

}
