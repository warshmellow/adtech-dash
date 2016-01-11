package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

trait HBaseClientInit {
  val hBaseConfig: Configuration = HBaseConfiguration.create
  hBaseConfig.set("hbase.master","127.0.0.1:16000")
  hBaseConfig.set("hbase.root", "hdfs://127.0.0.1:8020")
  hBaseConfig.set("hbase.zookeeper.quorum","127.0.0.1")
  hBaseConfig.set("hbase.zookeeper.property.clientPort","2181")
  hBaseConfig.set("zookeeper.znode.parent","/hbase-unsecure")
  hBaseConfig.set("hbase.master.port","16000")
  hBaseConfig.set("hbase.regionserver.port","16020")
}