package org.wquery.lang

import org.wquery.model._

import scala.util.parsing.combinator.RegexParsers

trait WTokenParsers extends RegexParsers {
  def tokenLit = (
    synsetLit ^^ { lit => (SynsetType, lit) }
    | senseLit ^^ { lit => (SenseType, lit) }
    | backQuotedString ^^ { lit => (StringType, false, lit) }
    | quotedString ^^ { lit => (StringType, true, lit) }
    | notQuotedString ^^ { lit => (StringType, true, lit) }
    | signedFloatNum ^^ { lit => (FloatType, lit) }
    | signedIntegerNum ^^ { lit => (IntegerType, lit) }
  )

  def synsetLit = "{" ~> senseLit <~ "}"

  def senseLit = alphaLit ~ ":" ~ integerNum ~ ":" ~ alphaLit ^^ {
    case word~_~num~_~pos => (word, num, pos)
  }

  def signedFloatNum = opt("-") ~ floatNum ^^ {
    case None~num => num
    case _~num => -num
  }

  def signedIntegerNum = opt("-") ~ integerNum ^^ {
    case None~num => num
    case _~num => -num
  }

  def alphaLit = (
    backQuotedString
      | quotedString
      | notQuotedString
    )

  def floatNum: Parser[Double] = {
    "([0-9]+(([eE][+-]?[0-9]+)|\\.(([0-9]+[eE][+-]?[0-9]+)|([eE][+-]?[0-9]+)|([0-9]+))))|(\\.[0-9]+([eE][+-]?[0-9]+)?)".r ^^ { _.toDouble }
  }

  def integerNum: Parser[Int] = "[0-9]+".r ^^ { _.toInt }
  def doubleQuotedString: Parser[String] = "\"(\\\\\"|[^\"])*?\"".r ^^ { x => escape(x.substring(1, x.length - 1), '"') }
  def backQuotedString: Parser[String] = "`(\\\\`|[^`])*?`".r ^^ { x => escape(x.substring(1, x.length - 1), '`') }
  def quotedString: Parser[String] = "'(\\\\'|[^'])*?'".r ^^ { x => escape(x.substring(1, x.length - 1), '\'') }

  def notQuotedString: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val source = in.source
      val offset = in.offset
      val start = handleWhiteSpace(source, offset)
      var i = start

      if (i < source.length && (source.charAt(i).isLetter || source.charAt(i) == '_')) {
        i += 1

        while (i < source.length && (source.charAt(i).isLetterOrDigit || source.charAt(i) == '_')) {
          i += 1
        }

        Success(source.subSequence(start, i).toString, in.drop(i - offset))
      } else {
        Failure("a letter expected but `" + in.first + "' found", in.drop(start - offset))
      }
    }
  }

  def escape(s: String, q: Char) = {
    val builder = new StringBuilder
    var idx = 0

    while (idx < s.size - 1) {
      val c = s.charAt(idx)
      val d = s.charAt(idx + 1)

      if (c == '\\') {
        if (d == 't') {
          builder.append('\t')
        } else if (d == 'n') {
          builder.append('\n')
        } else if (d == '\\') {
          builder.append('\\')
        } else if (d == q) {
          builder.append(q)
        } else {
          builder.append(c)
          builder.append(d)
        }

        idx += 2
      } else {
        builder.append(c)
        idx += 1
      }
    }

    if (idx == s.size - 1)
      builder.append(s.last)

    builder.toString()
  }
}
