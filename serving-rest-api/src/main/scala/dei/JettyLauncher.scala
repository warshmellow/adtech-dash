package dei  // remember this package in the sbt project definition
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import scala.util.{Failure, Success, Try}

object JettyLauncher { // this is my entry object as specified in sbt project definition
  def main(args: Array[String]) {
    // val port = if(System.getenv("PORT") != null) System.getenv("PORT").toInt else 8080

    val port = Try(args(0).toInt) match {
      case Success(n) => n
      case Failure(e) => 80
    }
    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")

    server.setHandler(context)

    server.start
    server.join
  }
}