package org.wquery.lang.parsers

import scala.util.parsing.combinator.RegexParsers
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import org.wquery.lang.exprs._

trait WParsers extends RegexParsers {

  def expr: Parser[EvaluableExpr] 

  def parse(input: String): EvaluableExpr = {
    parseAll(expr, input) match {
      case this.Success(result, _) =>
        result
      case this.Failure(message, _) =>
        throw new WQueryParsingFailureException(message)
      case this.Error(message, _) =>
        throw new WQueryParsingErrorException(message)
    }
  }
}
