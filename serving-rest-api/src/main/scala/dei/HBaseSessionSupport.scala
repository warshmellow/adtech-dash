package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.scalatra.ScalatraBase


trait HBaseSessionSupport { this: ScalatraBase =>
  def hBaseConfig: Configuration
  var conn: Connection = null

  before() {
    conn = ConnectionFactory.createConnection(hBaseConfig)
  }

  after() {
    conn.close()
  }

}