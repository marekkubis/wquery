package org.wquery.update.parsers

import org.wquery.model.Relation
import org.wquery.query.parsers.WQueryParsers
import org.wquery.update._
import org.wquery.update.exprs._

trait WUpdateParsers extends WQueryParsers {

  override def statement = (
    update
    | merge
    | super.statement
  )

  def update = "update" ~> (
    rel_spec ~ update_op ~ multipath_expr ^^ { case spec~op~expr => UpdateExpr(None, spec, op, expr) }
    | multipath_expr ~ rel_spec ~ update_op ~ multipath_expr ^^ { case lexpr~spec~op~rexpr => UpdateExpr(Some(lexpr), spec, op, rexpr) }
  )

  def rel_spec = (
    "^" ~> rel_spec_arg ^^ {
      spec => RelationSpecification(ConstantRelationSpecificationArgument(Relation.Dst)::spec::ConstantRelationSpecificationArgument(Relation.Src)::Nil)
    }
    | rep1sep(rel_spec_arg, "^") ^^ { RelationSpecification(_) }
  )

  def rel_spec_arg = (
    var_decl ^^ { VariableRelationSpecificationArgument(_) }
    | notQuotedString ^^ { ConstantRelationSpecificationArgument(_) }
  )

  def update_op = ("+="|"-="|":=")

  def merge = "merge" ~> expr ^^ { expr => MergeExpr(expr) }

}

