// scalastyle:off null

package org.wquery
import java.util.Properties

object WQueryProperties {
  private val properties = new Properties
  private val resourceStream = classOf[WQuery].getResourceAsStream("/wquery.properties")

  if (resourceStream != null) {
    properties.load(resourceStream)
    resourceStream.close
  }

  def version = properties.getProperty("wquery.version")

  def copyright = properties.getProperty("wquery.copyright")
}
