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
  def doubleQuotedString: Parser[String] = "\"(\\\\\"|[^\"])*?\"".r ^^ { x => x.substring(1, x.length - 1).replaceAll("\\\"","\"") }
  def backQuotedString: Parser[String] = "`(\\\\`|[^`])*?`".r ^^ { x => x.substring(1, x.length - 1).replaceAll("\\`","`") }
  def quotedString: Parser[String] = "'(\\\\'|[^'])*?'".r ^^ { x => x.substring(1, x.length - 1).replaceAll("\\'","'") }

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
}
