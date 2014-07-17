package org.wquery.utils

import java.io.{BufferedWriter, FileWriter}

object FileUtils {
  def dump(content: String, outputFileName: String) {
    val output = new BufferedWriter(new FileWriter(outputFileName))
    output.write(content)
    output.close()
  }
}
