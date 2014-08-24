package org.wquery.reader

class ExpressionReader(reader: LineReader) {
  type ExpressionState = (Boolean, Boolean, Boolean, Int)

  private def stripComments(s: String): String = {
    if (s == null)
      return s

    if (s.startsWith("#"))
      return ""

    val commentIndex = s.indexOf("--")

    if (commentIndex == -1)
      s
    else
      s.substring(0, commentIndex)
  }

  private def isLastLine(es: ExpressionState) = {
    es match {
      case (false, false, false, 0) =>
        true
      case _ =>
        false
    }
  }

  private def skipQoute(line: String, begin: Int, quote: Char): (Int, Boolean) = {
    var idx = begin

    for (idx <- begin until line.size) {
      if (line(idx) == quote && (idx == begin || line(idx - 1) != '\\'))
        return (idx + 1, false)
    }

    (line.size, true)
  }

  private def updateExpressionState(es: ExpressionState, line: String) = {
    var (inSingleQuote, inDoubleQuote, inBackQuote, doCount) = es
    var idx = 0

    while (idx < line.size) {
      if (inSingleQuote) {
        val (newIdx, newInQuote) = skipQoute(line, idx, '\'')

        idx = newIdx
        inSingleQuote = newInQuote
      } else if (inDoubleQuote) {
        val (newIdx, newInQuote) = skipQoute(line, idx, '"')

        idx = newIdx
        inDoubleQuote = newInQuote
      } else if (inBackQuote) {
        val (newIdx, newInQuote) = skipQoute(line, idx, '`')

        idx = newIdx
        inBackQuote = newInQuote
      } else  {
        line(idx) match {
          case '\'' =>
            inSingleQuote = true
          case '"' =>
            inDoubleQuote = true
          case '`' =>
            inBackQuote = true
          case _ =>

        }

        // do
        if ((idx < line.size - 1) && line(idx) == 'd' && line(idx + 1) == 'o'
          && (idx == 0 || !line(idx - 1).isLetterOrDigit)
          && (idx + 2 == line.size || !line(idx+2).isLetterOrDigit)) {
          doCount += 1
          idx += 1
        }

        // end
        if ((idx < line.size - 2) && line(idx) == 'e' && line(idx + 1) == 'n' && line(idx + 2) == 'd'
          && (idx == 0 || !line(idx - 1).isLetterOrDigit)
          && (idx + 3 == line.size || !line(idx+3).isLetterOrDigit)) {
          doCount -= 1
          idx += 2
        }

        idx += 1
      }
    }

    (inSingleQuote, inDoubleQuote, inBackQuote, doCount)
  }

  private def readExpression(): String = {
    val expression = new StringBuilder

    var line = stripComments(reader.readFirstLine)
    if (line == null) return line
    expression.append(line)
    expression.append('\n')
    var expressionState = updateExpressionState((false, false, false, 0), line)

    while (!isLastLine(expressionState)) {
      line = stripComments(reader.readNextLine)
      if (line == null) return expression.toString()
      expression.append(line)
      expression.append('\n')
      expressionState = updateExpressionState(expressionState, line)
    }

    expression.toString()
  }

  def foreach(f: String => Unit) {
    var done = false

    while(!done) {
      val expr = readExpression()

      if (expr == null)
        done = true
      else if (expr.trim().nonEmpty)
        f(expr)
    }
  }

  def close() = reader.close()
}
