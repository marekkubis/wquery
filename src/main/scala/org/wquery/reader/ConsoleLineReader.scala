package org.wquery.reader

import jline.console.ConsoleReader

class ConsoleLineReader(reader: ConsoleReader, prompt: String) extends LineReader {
  override def readFirstLine = reader.readLine(prompt)

  override def readNextLine = reader.readLine(" + ")

  override def close() {}
}
