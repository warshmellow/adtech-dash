package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection}
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

trait HBaseSessionTestSupport { this: ScalatraBase =>
  def hBaseTestingUtility: HBaseTestingUtility
  var conn: Connection = null

  before() {
    conn = hBaseTestingUtility.getConnection
  }
}