package org.wquery.lang.parsers

import org.wquery.lang.WTokenParsers
import org.wquery.lang.exprs._
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}

trait WLanguageParsers extends WTokenParsers {

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
