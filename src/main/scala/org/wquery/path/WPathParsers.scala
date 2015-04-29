package org.wquery.path.parsers

import org.wquery.lang.exprs._
import org.wquery.lang.parsers.WLanguageParsers
import org.wquery.model.{DataSet, Relation}
import org.wquery.path._
import org.wquery.path.exprs._
import org.wquery.path.operations._
import org.wquery.query.SetVariable

import scalaz.Scalaz._

trait WPathParsers extends WLanguageParsers {

  def expr = multipath_expr

  def multipath_expr: Parser[EvaluableExpr]
    = chainl1(intersect_expr,
              ("union"|"except") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinarySetExpr(x, l , r)) })

  def intersect_expr
    = chainl1(with_expr,
              "intersect" ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinarySetExpr(x, l , r)) })

  def with_expr
    = chainl1(add_expr, "," ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinarySetExpr(x, l , r)) }) ^^ { ConjunctiveExpr(_) }

  // arithmetics

  def add_expr
    = chainl1(mul_expr, ("+"|"-") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmeticExpr(x, l , r)) })

  def mul_expr
    = chainl1(unary_expr, ("*"|"/"|"mod"|"div") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmeticExpr(x, l , r)) })

  def unary_expr = (
    "-" ~> path ^^ { MinusExpr(_) }
    | "+" ~> path
    | path
  )

  def path = generator_subpath ~ rep(subpath) ^^ {
    case gpath~paths => paths.foldLeft(gpath){ case (expr, steps~projs) => PathExpr(NodeTransformationExpr(expr)::steps, projs) }
  }

  def generator_subpath = generator_step ~ rep(step) ~ rep(projection) ^^ { case gstep~steps~projs => PathExpr(gstep::steps, projs) }

  def subpath = rep1(step) ~ rep(projection)

  def projection = "<" ~> expr <~ ">" ^^ { ProjectionExpr(_) }

  def generator_step = generator  ^^ { NodeTransformationExpr(_) }

  def step = (
    relation_trans
    | node_trans
    | filter_trans
    | bind_trans
  )

  def relation_trans = "." ~> relation_union_expr ^^ { RelationTransformationExpr(_) }

  def relation_composition_expr = rep1sep(relation_union_expr, ".") ^^ { exprs => if (exprs.size == 1) exprs.head else RelationCompositionExpr(exprs) }

  def relation_union_expr: Parser[RelationalExpr] = rep1sep(quantified_relation_expr, "|") ^^ {
    exprs => if (exprs.size == 1) exprs.head else RelationUnionExpr(exprs)
  }

  def quantified_relation_expr = simple_relation_expr ~ quantifier ^^ {
    case expr~Quantifier(1, Some(1)) => expr
    case expr~quant => QuantifiedRelationExpr(expr, quant)
  }

  def simple_relation_expr = (
    "(" ~> relation_composition_expr <~ ")"
    | arc_expr
  )

  def arc_expr = (
    ("^" ~> notQuotedString) ^^ { id => ArcExpr(List(Relation.Dst, id, Relation.Src).map(ArcExprArgument(_, None))) } // syntactic sugar
    | rep1sep(arc_expr_arg, "^") ^^ { ArcExpr(_) }
  )

  def arc_expr_arg = notQuotedString ~ opt(("&" ~> notQuotedString)) ^^ { case name~dtype => ArcExprArgument(name, dtype) }

  def quantifier = (
    "+" ^^^ { Quantifier(1, None) }
    | "*" ^^^ { Quantifier(0, None) }
    | "?" ^^^ { Quantifier(0, Some(1)) }
    | "{" ~> "," ~> integerNum <~ "}" ^^ { x => Quantifier(0, Some(x)) }
    | "{" ~> integerNum ~ opt("," ~> opt(integerNum)) <~ "}" ^^ {
      case x~None => Quantifier(x, Some(x))
      case x~Some(None) => Quantifier(x, None)
      case x~Some(Some(y)) => Quantifier(x, Some(y))
    }
    | success(Quantifier(1, Some(1)))
  )

  def filter_trans = filter ^^ { FilterTransformationExpr(_) }

  def filter = "[" ~> or_condition <~ "]"

  def or_condition: Parser[ConditionalExpr] = rep1sep(and_condition, "or") ^^ { conds => if (conds.size == 1) conds.head else OrExpr(conds) }

  def and_condition = rep1sep(not_condition, "and") ^^ { conds => if (conds.size == 1) conds.head else AndExpr(conds) }

  def not_condition = (
    "not" ~> condition ^^ { NotExpr(_) }
    | condition
  )

  def condition = (
    "(" ~> or_condition <~ ")"
    | comparison
    | path ^^ { PathConditionExpr(_) }
  )

  def comparison = expr ~ ("@<<<"|"@<<"|"<<<"|"<<"|"<="|"<"|">="|">"|"=~"|"@!==="|"@!=="|"@!="|"@==="|"@=="|"@="|"==="|"=="|"="|"!==="|"!=="|"!="|"in") ~ expr ^^ {
    case lexpr~op~rexpr => BinaryConditionalExpr(op, lexpr, rexpr)
  }

  def node_trans = "." ~> non_rel_expr_generator ^^ { NodeTransformationExpr(_) }

  def bind_trans = rep1(step_var_decl|path_var_decl) ^^ { vars => BindTransformationExpr(VariableTemplate(vars)) }

  // generators
  def generator = (
    non_rel_expr_generator
    | rel_expr_generator
  )

  def non_rel_expr_generator = (
    boolean_generator
    | synset_generator
    | domain_generator
    | relation_generator
    | sense_generator
    | function_call_generator
    | float_generator
    | sequence_generator
    | integer_generator
    | back_generator
    | filter_generator
    | empty_set_generator
    | expr_generator
    | variable_generator
    | arc_generator
    | quoted_word_generator
  )

  def rel_expr_generator = relation_union_expr ^^ { ContextByRelationalExprReq(_) }

  def boolean_generator = (
    "true" ^^^ { AlgebraExpr(ConstantOp.fromValue(true)) }
    | "false" ^^^ { AlgebraExpr(ConstantOp.fromValue(false)) }
  )

  def synset_generator = (
    "{}" ^^^ { AlgebraExpr(FetchOp.synsets) }
    | "{" ~> expr <~ "}" ^^ { SynsetByExprReq(_) }
  )

  def domain_generator = "__" ^^^ { DomainReq() }

  def relation_generator = "!" ~> notQuotedString ^^ { RelationByNameReq(_) }

  def sense_generator = (
    "::" ^^ { _ => AlgebraExpr(FetchOp.senses) }
    | senseLit ^^ {
      case (word, num, pos) => SenseByWordFormAndSenseNumberAndPosReq(word, num, pos)
    }
    | alphaLit ~ ":" ~ integerNum ^^ {
      case word~_~num => AlgebraExpr(FetchOp.sensesByWordFormAndSenseNumber(word, num))
    }
  )

  def function_call_generator = notQuotedString ~ ("(" ~> opt(expr) <~ ")") ^^ { case name~y => FunctionExpr(name, y) }

  def quoted_word_generator = (
    backQuotedString ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
    | quotedString ^^ { value => AlgebraExpr(if (value === "") FetchOp.words else FetchOp.wordByValue(value)) }
    | doubleQuotedString ^^ { WordFormByRegexReq(_) }
  )

  def float_generator = floatNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def sequence_generator = integerNum ~ ".." ~ integerNum ^^ { case left~_~right => AlgebraExpr(ConstantOp(DataSet.fromList((left to right).toList))) }
  def integer_generator = integerNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def back_generator = "#" ^^^ { ContextByVariableReq(StepVariable.ContextVariable) }
  def filter_generator = filter ^^ { BooleanByFilterReq(_) }
  def empty_set_generator = "<>" ^^ { _ => AlgebraExpr(ConstantOp.empty) }
  def expr_generator = "(" ~> expr <~ ")"
  def variable_generator = var_decl ^^ { ContextByVariableReq(_) }
  def arc_generator = "\\" ~> arc_expr ^^ { ArcByArcExprReq(_) }

  // variables
  def var_decl = (
    step_var_decl
    | path_var_decl
    | set_var_decl
  )

  def step_var_decl = "$" ~> notQuotedString ^^ { StepVariable(_) }
  def path_var_decl = "@" ~> notQuotedString ^^ { TupleVariable(_) }
  def set_var_decl = "%" ~> notQuotedString ^^ { SetVariable(_) }
}
