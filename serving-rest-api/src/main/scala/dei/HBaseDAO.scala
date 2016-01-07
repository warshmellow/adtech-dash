package dei

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

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
      "classifierLastRetrained" -> metricsBundle.classifierLastRetrained.toString
    )
    HBaseObj(rowKey, columnFamily, columnValuePairs)
  }

  def toPut(hBaseObj: HBaseObj) = {
    val p = new Put(Bytes.toBytes(hBaseObj.rowKey))
    hBaseObj.columnValuePairs.foreach {
      case (column, value) =>
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

    Try(resultsAsBundles.toList) match {
      case Success(stuff) => stuff
      case Failure(e) => List()
    }
  }
}