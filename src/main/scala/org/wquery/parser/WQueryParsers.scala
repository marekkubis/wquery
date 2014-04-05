package org.wquery.parser

import org.wquery.path.exprs.FunctionExpr
import org.wquery.path.operations.{SortFunction, DistinctFunction}
import org.wquery.update.parsers.WUpdateParsers
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import org.wquery.model.DataSet
import org.wquery.model.Relation
import org.wquery.lang.exprs._
import scalaz._
import Scalaz._

trait WParsers extends WUpdateParsers {

  def query = (
    imp_expr
    | multipath_expr ^^ { x => FunctionExpr(SortFunction.name, FunctionExpr(DistinctFunction.name, x)) }
  )

  def parse(input: String): EvaluableExpr = {
    parseAll(query, input) match {
      case this.Success(expr, _) =>
        expr
      case this.Failure(message, _) =>
        throw new WQueryParsingFailureException(message)
      case this.Error(message, _) =>
        throw new WQueryParsingErrorException(message)
    }
  }
}
