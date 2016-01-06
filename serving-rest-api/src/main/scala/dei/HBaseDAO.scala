package dei

import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes

case class HBaseObj(
                     rowKey: Long,
                     columnFamily: String,
                     columnValuePairs: Map[String, String])

object HBaseDAO {
  def toHBaseObj(metricsBundle: ClassifierMetricsBundle): HBaseObj = {
    val rowKey = metricsBundle.timestamp
    val columnFamily = "d"
    val columnValuePairs = Map(
      "precision" -> metricsBundle.precision.toString,
      "recall" -> metricsBundle.recall.toString,
      "f1" -> metricsBundle.f1.toString,
      "classifierLastRetrained" -> metricsBundle.classiferLastRetrained.toString
    )
    HBaseObj(rowKey, columnFamily, columnValuePairs)
  }

  def toPut(hBaseObj: HBaseObj) = {
    val p = new Put(Bytes.toBytes(hBaseObj.rowKey))
    hBaseObj.columnValuePairs.foreach {
      case (column, value) => {
        p.addColumn(Bytes.toBytes(hBaseObj.columnFamily),
          Bytes.toBytes(column),
          Bytes.toBytes(value))
      }
    }
    p
  }

  def put(table: Table, hBaseObj: HBaseObj) = table.put(toPut(hBaseObj))
}