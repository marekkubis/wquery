package org.wquery.query.parsers

import org.wquery.lang.exprs._
import org.wquery.path.parsers.WPathParsers
import org.wquery.query.exprs._

trait WQueryParsers extends WPathParsers {

  override def expr = (
    imp_expr
    | super.expr
  )

  def imp_expr: Parser[EvaluableExpr] = (
    "do" ~> rep(statement) <~ "end" ^^ { BlockExpr(_) }
    | statement
  )

  def statement: Parser[EvaluableExpr] = (
    iterator
    | emission
    | assignment
    | ifelse
    | whiledo
    | function
  )

  def iterator = "from" ~> multipath_expr ~ imp_expr ^^ { case mexpr~iexpr => IteratorExpr(mexpr, iexpr) }

  def emission = "emit" ~> multipath_expr ^^ { EmissionExpr(_) }

  def assignment = set_var_decl ~ ":=" ~ multipath_expr ^^ { case vdecl~_~mexpr => VariableAssignmentExpr(vdecl, mexpr) }

  def ifelse = "if" ~> multipath_expr ~ imp_expr ~ opt("else" ~> imp_expr) ^^ {
    case cexpr ~ iexpr ~ eexpr => IfElseExpr(cexpr, iexpr, eexpr)
  }

  def whiledo = "while" ~> multipath_expr ~ imp_expr ^^ { case cexpr~iexpr => WhileDoExpr(cexpr, iexpr) }

  def function = "function" ~> notQuotedString ~ imp_expr ^^ { case cexpr~iexpr => FunctionDefinitionExpr(cexpr, iexpr) }

}
