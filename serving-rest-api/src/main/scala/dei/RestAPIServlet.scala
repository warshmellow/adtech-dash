package dei

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.scalatra.ScalatraServlet
import org.scalatra.scalate.ScalateSupport

// JSON-related libraries
import org.json4s.{DefaultFormats, Formats, JValue}
// JSON handling support from Scalatra
import org.scalatra.json._

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
  extends RestAPIServlet with HBaseSessionSupport with RestAPIServletRoutes
  with RestAPIServletViewRoutes {

  def hBaseConfig = hBaseConf
  def connection = conn
}

case class RestAPIServletWithHBaseTestingUtil(hBaseTestingUtil: HBaseTestingUtility)
  extends RestAPIServlet with HBaseSessionTestSupport with RestAPIServletRoutes
  with RestAPIServletViewRoutes {

  def hBaseTestingUtility = hBaseTestingUtil
  def connection = conn
}

trait RestAPIServletRoutes extends ScalatraServlet {
  def connection: Connection

  get("/classifier/metrics.json") {
    (params.get("since"), params.get("until")) match {
      case (None,_) => halt(400)
      case (_, None) => halt(400)
      case _ => None
    }

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

trait RestAPIServletViewRoutes extends ScalatraServlet with ScalateSupport {
  get("/") {
    contentType = "text/html"
    layoutTemplate("/views/index.ssp")
  }
}