package dei

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatra.ScalatraBase
import org.scalatra.test.scalatest.ScalatraFlatSpec


trait HBaseSessionTestSupport { this: ScalatraBase =>
  def hBaseTestingUtility: HBaseTestingUtility
  var conn: Connection = null

  before() {
    conn = hBaseTestingUtility.getConnection
  }
}

case class RestAPIServletWithHBaseTestingUtil(hBaseTestingUtil: HBaseTestingUtility)
  extends RestAPIServlet with HBaseSessionTestSupport with RestAPIServletRoutes
  with RestAPIServletViewRoutes {

  def hBaseTestingUtility = hBaseTestingUtil
  def connection = conn
}

@RunWith(classOf[JUnitRunner])
class RestAPIServletSpec extends ScalatraFlatSpec
  with Matchers with BeforeAndAfterAll with MockitoSugar {
  // `RestAPIServlet` is your app which extends ScalatraServlet
  // addServlet(new RestAPIServletWithHBaseConfig(HBaseConfiguration.create), "/*")
  // addServlet(classOf[RestAPIServlet], "/*")
  val utility = new HBaseTestingUtility


  addServlet(new RestAPIServletWithHBaseTestingUtil(utility), "/*")

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  override def beforeAll(): Unit = {
    super.beforeAll()
    utility.startMiniCluster()

    // Define "metrics" table
    val table = utility.createTable(Bytes.toBytes("metrics"), Bytes.toBytes("d"))
    // Define contents of "metrics" table
    val metricsBundles = List(
      ClassifierMetricsBundle(
      timestamp= 1451793410L,
      auPRC=0.3,
      classifierLastRetrained= 1451793400L),
      ClassifierMetricsBundle(
        timestamp= 1451793420L,
        auPRC=0.3,
        classifierLastRetrained= 1451793400L),
      ClassifierMetricsBundle(
        timestamp= 1451793430L,
        auPRC=0.4,
        classifierLastRetrained= 1451793400L)
    )
    // Insert contents into "metrics" table
    metricsBundles.foreach(record => HBaseDAO.put(table, HBaseDAO.toHBaseObj(record)))
  }

  behavior of "GET classifier/metrics.json"
  it should "respond with bad request to JSON without parameters" in {
    get("/classifier/metrics.json") {
      status should equal(400)
    }
  }

  it should "respond with JSON when given parameters since and until" in {
    val emptyStartRowKey: Long = 1451793300L
    val emptyStopRowKey: Long = 1451793399L

    val nonEmptyStartRowKey: Long = 1451793404L
    val nonEmptyStopRowKey: Long = 1451793422L


    get("/classifier/metrics.json",
      Map("since" -> emptyStartRowKey.toString, "until" -> emptyStopRowKey.toString)) {
      status should equal(200)
      parse(body).extract[ClassifierMetricsBundleSeq] shouldBe empty
    }

    get("/classifier/metrics.json",
      Map("since" -> nonEmptyStartRowKey.toString, "until" -> nonEmptyStopRowKey.toString)) {
      status should equal(200)
      parse(body).extract[ClassifierMetricsBundleSeq] should not be empty
    }
  }

  it should "respond with empty JSON when given parameters since >= until" in {
    get("/classifier/metrics.json",
      Map("since" -> "1451793500", "until" -> "1451793500")) {
      status should equal(200)
      parse(body).extract[ClassifierMetricsBundleSeq] shouldBe empty
    }
  }

  ignore should "respond with bad request on non Long since/until" in {
    get("/classifier/metrics.json",
      Map("since" -> "foo", "until" -> "bar")) {
      status should equal(400)
    }
  }
}