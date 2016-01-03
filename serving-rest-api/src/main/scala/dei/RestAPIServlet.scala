package dei

import org.scalatra.ScalatraServlet
// JSON-related libraries
import org.json4s.{JValue, DefaultFormats, Formats}
// JSON handling support from Scalatra
import org.scalatra.json._

case class ClassifierMetricsBundle(
                                    precision: Double,
                                    recall: Double,
                                    f1: Double,
                                    timestamp: Long,
                                    classiferLastRetrained: Long)

case class ClassifierMetricsBundleSeq(classifierMetricsBundles: Seq[ClassifierMetricsBundle]) {
  def size() = classifierMetricsBundles.size
  def isEmpty() = size == 0
}

class RestAPIServlet extends ScalatraServlet with JacksonJsonSupport {

  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  get("/classifier/metrics.json") {
    val since = params.getOrElse("since", "1451793414").toInt
    val until = params.getOrElse("until", "1451793501").toInt

    if (since < until) {
      val resultList = List(
        ClassifierMetricsBundle(
          precision= 0.6,
          recall= 0.7,
          f1= 0.8,
          timestamp= 1451793414,
          classiferLastRetrained= 1451793400),
        ClassifierMetricsBundle(
          precision= 0.61,
          recall= 0.71,
          f1= 0.81,
          timestamp= 1451793474,
          classiferLastRetrained= 1451793400))

      ClassifierMetricsBundleSeq(resultList)
    }
    else ClassifierMetricsBundleSeq(List())
  }

  notFound {
    serveStaticResource() getOrElse resourceNotFound()
  }

  override def render(value: JValue)(implicit formats: Formats): JValue =
    value.camelizeKeys
}