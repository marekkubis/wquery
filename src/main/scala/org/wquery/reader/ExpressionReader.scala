package org.wquery.reader

class ExpressionReader(reader: LineReader) {
  private def readExpression: String = {
    val s = reader.readFirstLine

    if (s == null || s == "")
      s
    else
      s + "\n" + readExpression
  }

  def foreach(f: String => Unit) {
    var done = false

    while(!done) {
      val expr = readExpression

      if (expr == null)
        done = true
      else
        f(expr)
    }
  }

  def close() = reader.close()
}
