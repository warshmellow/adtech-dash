package dei

import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatra.test.scalatest.ScalatraFlatSpec

@RunWith(classOf[JUnitRunner])
class RestAPIServletSpec extends ScalatraFlatSpec with Matchers {
  // `RestAPIServlet` is your app which extends ScalatraServlet
  addServlet(classOf[RestAPIServlet], "/*")

  behavior of "GET classifier/metrics.json"
  it should "respond with JSON" in {
    get("/classifier/metrics.json") {
      status should equal (200)
    }
  }
}
