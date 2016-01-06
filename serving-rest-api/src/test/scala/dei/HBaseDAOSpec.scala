package dei

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, ConnectionFactory, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.{ArgumentCaptor, Captor, Mock}
import org.mockito.Mockito._
import org.mockito.runners.MockitoJUnitRunner
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatra.test.scalatest.ScalatraFlatSpec

@RunWith(classOf[MockitoJUnitRunner])
class HBaseDAOSpec extends ScalatraFlatSpec with Matchers with MockitoSugar {

  @Mock val conf = HBaseConfiguration.create
  @Mock val connection = ConnectionFactory.createConnection(conf)
  @Mock var table: Table = null
  @Captor var putCaptor: ArgumentCaptor[Put] = null

  @Test
  def testHello() = { assert(0 == 0)}

  @Test
  def testInsertRecord() = {
    when(connection.getTable(TableName.valueOf("tablename"))).thenReturn(table)

    val metricsBundle = ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793414,
      classiferLastRetrained= 1451793400)

    val obj = HBaseDAO.toHBaseObj(metricsBundle)

    HBaseDAO.put(table, obj)
    verify(table).put(putCaptor.capture())
    val put = putCaptor.getValue()

    assertResult(obj.rowKey) {
      Bytes.toString(put.getRow())
    }
  }
}