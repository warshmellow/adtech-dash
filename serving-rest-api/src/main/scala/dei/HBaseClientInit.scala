package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

trait HBaseClientInit {
  val hBaseConfig: Configuration = HBaseConfiguration.create
  hBaseConfig.set("zookeeper.znode.parent","/hbase-unsecure")
}