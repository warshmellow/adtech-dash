package dei

import org.apache.hadoop.hbase.HBaseConfiguration
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatra.test.scalatest.ScalatraFlatSpec


@RunWith(classOf[JUnitRunner])
class RestAPIServletSpec extends ScalatraFlatSpec
  with Matchers with BeforeAndAfterAll with MockitoSugar {
  // `RestAPIServlet` is your app which extends ScalatraServlet
  addServlet(new RestAPIServletWithHBaseConfig(HBaseConfiguration.create), "/*")

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  behavior of "GET classifier/metrics.json"
  it should "respond with JSON without parameters" in {
    get("/classifier/metrics.json") {
      status should equal(200)
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
  }

  it should "respond with empty JSON when given parameters since >= until" in {
    get("/classifier/metrics.json",
      Map("since" -> "1451793500", "until" -> "1451793500")) {
      status should equal(200)
      parse(body).extract[ClassifierMetricsBundleSeq] shouldBe empty
    }
  }

  it should "respond with bad request on non Long since/until" in {
    get("/classifier/metrics.json",
      Map("since" -> "foo", "until" -> "bar")) {
      status should equal(400)
    }
  }
}