package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.scalatra.ScalatraServlet

// JSON-related libraries
import org.json4s.{DefaultFormats, Formats, JValue}
// JSON handling support from Scalatra
import org.scalatra.json._

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


class RestAPIServlet extends ScalatraServlet with JacksonJsonSupport {

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  notFound {
    serveStaticResource() getOrElse resourceNotFound()
  }

  override def render(value: JValue)(implicit formats: Formats): JValue =
    value.camelizeKeys
}


case class RestAPIServletWithHBaseConfig(hBaseConf: Configuration)
  extends RestAPIServlet with HBaseSessionSupport with RestAPIServletRoutes {

  def hBaseConfig = hBaseConf
  def connection = conn
}

case class RestAPIServletWithHBaseTestingUtil(hBaseTestingUtil: HBaseTestingUtility)
  extends RestAPIServlet with HBaseSessionTestSupport with RestAPIServletRoutes {

  def hBaseTestingUtility = hBaseTestingUtil
  def connection = conn
}

trait RestAPIServletRoutes extends ScalatraServlet {
  def connection: Connection

  get("/classifier/metrics.json") {
    val startRowKey = params("since").toLong
    val stopRowKey = params("until").toLong

    val table = connection.getTable(TableName.valueOf("metrics"))

    val scanResults = HBaseDAO.scan(table, startRowKey, stopRowKey).
      filter(x => x.isDefined).
      map {
      case Some(bundle) => bundle
      case None => None }

    Map("classifierMetricsBundles" -> scanResults)
  }
}