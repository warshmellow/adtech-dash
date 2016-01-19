package dei

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

case class ClassifierMetricsBundle(
                                    precision: Double,
                                    recall: Double,
                                    f1: Double,
                                    timestamp: Long,
                                    classifierLastRetrained: Long)

case class ClassifierMetricsBundleSeq(classifierMetricsBundles: Seq[ClassifierMetricsBundle]) {
  def size() = classifierMetricsBundles.size
  def isEmpty = size == 0
}

case class HBaseObj(
                     rowKey: Long,
                     columnFamily: String,
                     columnValuePairs: Map[String, Either[Double, Long]])

object HBaseDAO {
  def toHBaseObj(metricsBundle: ClassifierMetricsBundle): HBaseObj = {
    val rowKey = metricsBundle.timestamp
    val columnFamily = "d"
    val columnValuePairs = Map(
      "precision" -> Left(metricsBundle.precision),
      "recall" -> Left(metricsBundle.recall),
      "f1" -> Left(metricsBundle.f1),
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

  def toGet(rowKey: Long) = new Get(Bytes.toBytes(rowKey))

  def put(table: Table, hBaseObj: HBaseObj) = table.put(toPut(hBaseObj))

  def get(table: Table, rowKey: Long): Option[ClassifierMetricsBundle] = {
    resultToClassifierMetricsBundle(table.get(toGet(rowKey)))
  }

  def resultToClassifierMetricsBundle(result: Result):
    Option[ClassifierMetricsBundle] = {
    if (result.isEmpty) None
    else {
      val paramVal = List("precision", "recall", "f1", "timestamp", "classifierLastRetrained").map {
        x =>
          if (x == "timestamp") (x, result.getRow)
          else (x, result.getValue(Bytes.toBytes("d"), Bytes.toBytes(x)))
      }
      val paramValMap = paramVal.toMap

      val p = Bytes.toDouble(paramValMap("precision"))
      val r = Bytes.toDouble(paramValMap("recall"))
      val f = Bytes.toDouble(paramValMap("f1"))
      val t = Bytes.toLong(paramValMap("timestamp"))
      val c = Bytes.toLong(paramValMap("classifierLastRetrained"))

      Some(ClassifierMetricsBundle(
        precision= p,
        recall= r,
        f1= f,
        timestamp= t,
        classifierLastRetrained= c
      ))
    }
  }

  def scan(table: Table, startRowKey: Long, stopRowKey: Long):
    Seq[Option[ClassifierMetricsBundle]] = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(startRowKey))
    scan.setStopRow(Bytes.toBytes(stopRowKey))

    scan.setCaching(500)
    val scanner = table.getScanner(scan)
    val resultsAsBundles = scanner.map(resultToClassifierMetricsBundle)
    scanner.close()

    resultsAsBundles.toList
  }
}
