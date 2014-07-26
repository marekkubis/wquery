package org.wquery.reader

import java.io._

class InputLineReader(input: Reader) extends LineReader {
  val bufferedInput = new BufferedReader(input)

  override def readFirstLine = bufferedInput.readLine()

  override def readNextLine = bufferedInput.readLine()

  override def close() = input.close()
}
