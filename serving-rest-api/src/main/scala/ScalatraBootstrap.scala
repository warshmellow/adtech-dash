import javax.servlet.ServletContext

import dei.{HBaseClientInit, RestAPIServletWithHBaseConfig}
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle with HBaseClientInit {

  override def init(context: ServletContext) {
    context mount (new RestAPIServletWithHBaseConfig(hBaseConfig), "/*")
  }
}