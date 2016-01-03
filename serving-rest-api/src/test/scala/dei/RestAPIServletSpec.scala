package dei

import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatra.test.scalatest.ScalatraFlatSpec

import org.json4s._
import org.json4s.jackson.JsonMethods._

@RunWith(classOf[JUnitRunner])
class RestAPIServletSpec extends ScalatraFlatSpec with Matchers {
  // `RestAPIServlet` is your app which extends ScalatraServlet
  addServlet(classOf[RestAPIServlet], "/*")

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  behavior of "GET classifier/metrics.json"
  it should "respond with JSON without parameters" in {
    get("/classifier/metrics.json") {
      status should equal (200)
    }
  }

  it should "respond with JSON when given parameters since and until" in {
    get("/classifier/metrics.json",
      Map("since" -> "1451793412", "until" -> "1451793500")) {
      status should equal (200)
      parse(body).extract[ClassifierMetricsBundleSeq]
    }
  }

  it should "respond with empty JSON when given parameters since >= until" in {
    get("/classifier/metrics.json",
      Map("since" -> "1451793500", "until" -> "1451793500")) {
      status should equal (200)
      parse(body).extract[ClassifierMetricsBundleSeq] shouldBe empty
    }
  }
}
