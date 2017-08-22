

## hbase-rdd examples



## shc examples

官方示例:

bin/spark-submit --master yarn-cluster \
--class org.apache.spark.sql.execution.datasources.hbase.examples.HBaseSource \
--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 \
--repositories http://repo.hortonworks.com/content/groups/public/ \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml \
shc-examples-1.1.1-2.1-s_2.11-SNAPSHOT.jar

测试示例:

/usr/install/spark-2.1.1-bin-hadoop2.7/bin/spark-submit \
--class shc.examples.HBaseSource \
--files /usr/install/hbase1.2/conf/hbase-site.xml \
hbase2-examples-1.0-SNAPSHOT-jar-with-dependencies.jar