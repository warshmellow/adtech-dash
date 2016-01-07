package dei

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.junit.{Ignore, Test}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{ArgumentCaptor, Captor, Mock}
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatra.test.scalatest.ScalatraFlatSpec

@RunWith(classOf[MockitoJUnitRunner])
class HBaseDAOSpec extends ScalatraFlatSpec with Matchers with MockitoSugar {

  @Mock val conf = HBaseConfiguration.create
  @Mock val connection = ConnectionFactory.createConnection(conf)
  @Mock var table: Table = null
  @Mock var result: Result = null
  @Captor var putCaptor: ArgumentCaptor[Put] = null

  @Test
  def testInsertRecord() = {
    when(connection.getTable(TableName.valueOf("tablename"))).thenReturn(table)

    val metricsBundle = ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793414L,
      classifierLastRetrained= 1451793400L)

    val obj = HBaseDAO.toHBaseObj(metricsBundle)

    // Use HBaseDAO to insert record
    HBaseDAO.put(table, obj)
    verify(table).put(putCaptor.capture())
    val put = putCaptor.getValue

    // Assert Row Key of Put is correct
    assertResult(obj.rowKey) {
      Bytes.toLong(put.getRow)
    }
    // Assert Column Family and Columns of Put are correct
    List("precision", "recall", "classifierLastRetrained").foreach { x =>
      assert(put.has(Bytes.toBytes("d"), Bytes.toBytes(x)))
    }
    // Assert Values of Put are correct
    Map(
      "precision" -> "0.6",
      "recall" -> "0.7",
      "f1" -> "0.8",
      "classifierLastRetrained" -> "1451793400").
    foreach {
      case (col, value) => assertResult(value) {
        Bytes.toString(
          put.get(Bytes.toBytes("d"), Bytes.toBytes(col)).get(0).getValue)
      }
    }
  }

  @Test
  def testGetRecord() = {
    // On empty row key, results should be empty
    // On nonempty row key, results should be non empty
    when(connection.getTable(TableName.valueOf("tablename"))).thenReturn(table)

    // "Contents" of table
    val metricsBundle = ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793414L,
      classifierLastRetrained= 1451793400L)

    val emptyRowKey: Long = 1451793400L
    val nonEmptyRowKey: Long = 1451793414L

    when(table.get(HBaseDAO.toGet(emptyRowKey))).thenReturn(result)
    when(result.isEmpty).thenReturn(true)

    val emptyResult = HBaseDAO.get(table, emptyRowKey)

    assertResult(true) { emptyResult.isEmpty }


    when(table.get(HBaseDAO.toGet(nonEmptyRowKey))).thenReturn(result)
    when(result.isEmpty).thenReturn(false)
    when(result.getRow).thenReturn(Bytes.toBytes(1451793414L))
    when(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("precision"))).
      thenReturn(Bytes.toBytes(0.6))
    when(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("recall"))).
      thenReturn(Bytes.toBytes(0.7))
    when(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("f1"))).
      thenReturn(Bytes.toBytes(0.8))
    when(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("classifierLastRetrained"))).
      thenReturn(Bytes.toBytes(1451793400L))

    val nonEmptyResult = HBaseDAO.get(table, nonEmptyRowKey)

    assertResult(false) { nonEmptyResult.isEmpty }
  }

  @Ignore @Test
  def testScanRecords() = {

    when(connection.getTable(TableName.valueOf("tablename"))).thenReturn(table)
    // "Contents" of table
    // Different timestamps
    val metricsBundle = List(
      ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793401L,
      classifierLastRetrained= 1451793400L),
    ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793404L,
      classifierLastRetrained= 1451793400L),
    ClassifierMetricsBundle(
      precision= 0.6,
      recall= 0.7,
      f1= 0.8,
      timestamp= 1451793420L,
      classifierLastRetrained= 1451793400L))

    val emptyStartRowKey: Long = 1451793300L
    val emptyStopRowKey: Long = 1451793399L
    val nonEmptyStartRowKey: Long = 1451793404L
    val nonEmptyStopRowKey: Long = 1451793422L

    val emptyRecords = HBaseDAO.scan(table, emptyStartRowKey, emptyStopRowKey)

    assertResult(true) { emptyRecords.isEmpty }

    val nonEmptyRecords = HBaseDAO.scan(table, nonEmptyStartRowKey, nonEmptyStopRowKey)
    assertResult(false) { nonEmptyRecords.isEmpty }
  }
}