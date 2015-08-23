package org.wquery.query.parsers

import org.wquery.lang.exprs._
import org.wquery.path.parsers.WPathParsers
import org.wquery.query.SetVariable
import org.wquery.query.exprs._

trait WQueryParsers extends WPathParsers {

  override def expr = (
    imp_expr
    | super.expr
  )

  override def var_decl = (
    set_var_decl
    | super.var_decl
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

  def set_var_decl = "%" ~> notQuotedString ^^ { SetVariable(_) }

  def set_var_decls = rep1sep(set_var_decl, ",")

  def assignment = set_var_decls ~ ":=" ~ multipath_expr ^^ { case vdecls~_~mexpr => VariableAssignmentExpr(vdecls, mexpr) }

  def ifelse = "if" ~> multipath_expr ~ imp_expr ~ opt("else" ~> imp_expr) ^^ {
    case cexpr ~ iexpr ~ eexpr => IfElseExpr(cexpr, iexpr, eexpr)
  }

  def whiledo = "while" ~> multipath_expr ~ imp_expr ^^ { case cexpr~iexpr => WhileDoExpr(cexpr, iexpr) }

  def function = "function" ~> notQuotedString ~ imp_expr ^^ { case cexpr~iexpr => FunctionDefinitionExpr(cexpr, iexpr) }

}
