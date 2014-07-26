package org.wquery.reader

class ExpressionReader(reader: LineReader) {
  def readExpression: String = {
    val s = reader.readFirstLine

    if (s == null || s == "")
      return s
    else
      return s + "\n" + readExpression
  }
}
