package dei

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

case class ClassifierMetricsBundle(
                                    timestamp: Long,
                                    auPRC: Double,
                                    classifierLastRetrained: Long)

case class HBaseObj(
                     rowKey: Long,
                     columnFamily: String,
                     columnValuePairs: Map[String, Either[Double, Long]])

object HBaseDAO {
  def toHBaseObj(metricsBundle: ClassifierMetricsBundle): HBaseObj = {
    val rowKey = metricsBundle.timestamp
    val columnFamily = "d"
    val columnValuePairs = Map(
      "auPRC" -> Left(metricsBundle.auPRC),
      "classifierLastRetrained" -> Right(metricsBundle.classifierLastRetrained)
    )
    HBaseObj(rowKey, columnFamily, columnValuePairs)
  }

  def toPut(hBaseObj: HBaseObj) = {
    val p = new Put(Bytes.toBytes(hBaseObj.rowKey))
    hBaseObj.columnValuePairs.foreach {
      case (column, Left(value)) =>
        p.addColumn(Bytes.toBytes(hBaseObj.columnFamily),
          Bytes.toBytes(column),
          Bytes.toBytes(value))
      case (column, Right(value)) =>
        p.addColumn(Bytes.toBytes(hBaseObj.columnFamily),
          Bytes.toBytes(column),
          Bytes.toBytes(value))
    }
    p
  }

  def put(table: Table, hBaseObj: HBaseObj): Unit = table.put(toPut(hBaseObj))

  def put(table: Table, metricsBundle: ClassifierMetricsBundle): Unit =
    put(table, toHBaseObj(metricsBundle))

  def drop(table: Table, admin: Admin) = {
    admin.disableTable(table.getName)
    admin.deleteTable(table.getName)
  }
}