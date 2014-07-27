package org.wquery.reader

import jline.console.ConsoleReader

class ConsoleLineReader(reader: ConsoleReader) extends LineReader {
  override def readFirstLine = reader.readLine()

  override def readNextLine = reader.readLine(" + ")

  override def close() {}
}
