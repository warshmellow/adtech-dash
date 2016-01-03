package dei

import org.scalatra.ScalatraServlet
// JSON-related libraries
import org.json4s.{JValue, DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._

class RestAPIServlet extends ScalatraServlet with JacksonJsonSupport {

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  get("/classifier/metrics.json") {
    List("hello")
  }

  notFound {
    serveStaticResource() getOrElse resourceNotFound()
  }

  override def render(value: JValue)(implicit formats: Formats): JValue =
    value.camelizeKeys
}