package org.wquery.parser
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import scala.util.parsing.combinator.RegexParsers
import org.wquery.engine._
import org.wquery.engine.operations._
import org.wquery.model.DataSet
import scalaz._
import Scalaz._

trait WQueryParsers extends RegexParsers {

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

  // parsers

  def query = (
    imp_expr
    | multipath_expr ^^ { x => FunctionExpr(SortFunction.name, FunctionExpr(DistinctFunction.name, x)) }
  )

  def expr = (
    imp_expr
    | multipath_expr
  )

  // imperative exprs

  def imp_expr: Parser[EvaluableExpr] = (
    "do" ~> rep(statement) <~ "end" ^^ { BlockExpr(_) }
    | statement
  )

  def statement = (
    iterator
    | emission
    | merge
    | split
    | assignment
    | ifelse
    | whiledo
  )

  def iterator = "from" ~> multipath_expr ~ imp_expr ^^ { case mexpr~iexpr => IteratorExpr(mexpr, iexpr) }

  def emission = "emit" ~> multipath_expr ^^ { EmissionExpr(_) }

  def merge = "merge" ~> expr ~ rep(with_clause) ^^ { case expr~withs => MergeExpr(expr, withs) }

  def split = "split" ~> expr ~ rep(with_clause) ^^ { case expr~withs => SplitExpr(expr, withs) }

  def assignment = (
     var_decls ~ ":=" ~ multipath_expr ^^ { case vdecls~_~mexpr => VariableAssignmentExpr(vdecls, mexpr) }
     | "wordnet" ~> notQuotedString ~ assignment_op ~ multipath_expr ~ rep(with_clause) ^^ { case prop~op~rexpr~withs => WordNetUpdateExpr(prop, op, rexpr, withs) }
     | notQuotedString ~ ":=" ~ relation_union_expr  ^^ { case name~_~rexpr => RelationAssignmentExpr(name, rexpr) }
     | multipath_expr ~ arc_expr ~ assignment_op ~ multipath_expr ^^ { case lexpr~prop~op~rexpr => UpdateExpr(lexpr, prop, op, rexpr) }
  )

  def assignment_op = ("+="|"-="|":=")

  def with_clause = "with" ~> property_assignment

  def property_assignment = arc_expr ~ assignment_op ~ multipath_expr ^^ { case arc~op~expr => PropertyAssignmentExpr(arc, op, expr) }

  def ifelse = "if" ~> multipath_expr ~ imp_expr ~ opt("else" ~> imp_expr) ^^ {
    case cexpr ~ iexpr ~ eexpr => IfElseExpr(cexpr, iexpr, eexpr)
  }

  def whiledo = "while" ~> multipath_expr ~ imp_expr ^^ { case cexpr~iexpr => WhileDoExpr(cexpr, iexpr) }

  // mutlipath exprs

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
    = chainl1(unary_expr, ("*"|"/"|"%"|"div") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmeticExpr(x, l , r)) })

  def unary_expr = (
    "-" ~> path ^^ { MinusExpr(_) }
    | "+" ~> path
    | path
  )

  def path = generator_subpath ~ rep(subpath) ^^ { case gpath~paths => paths.foldLeft(gpath){ case (expr, steps~projs) => PathExpr(NodeTransformationExpr(expr)::steps, projs) }}

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

  def relation_trans = dots ~ relation_union_expr ^^ { case pos~expr => RelationTransformationExpr(pos, expr) }

  def dots = rep1(".") ^^ { _.size - 1 }

  def relation_composition_expr = rep1sep(relation_union_expr, ".") ^^ { exprs => if (exprs.size == 1) exprs.head else RelationCompositionExpr(exprs) }

  def relation_union_expr: Parser[RelationalExpr] = rep1sep(quantified_relation_expr, "|") ^^ { exprs => if (exprs.size == 1) exprs.head else RelationUnionExpr(exprs) }

  def quantified_relation_expr = simple_relation_expr ~ quantifier ^^ { 
    case expr~Quantifier(1, Some(1)) => expr
    case expr~quant => QuantifiedRelationExpr(expr, quant)  
  }

  def simple_relation_expr = (
    "(" ~> relation_composition_expr <~ ")"
    | arc_expr
    | "\\" ~> step_var_decl ^^ { VariableRelationalExpr(_) }
  )

  def arc_expr = (
    ("^" ~> notQuotedString) ^^ { id => ArcExpr(List("destination", id, "source").map(ArcExprArgument(_, None))) } // syntactic sugar
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

  def comparison = expr ~ ("<="|"<"|">="|">"|"=~"|"="|"!="|"in"|"pin") ~ expr ^^ {
    case lexpr~op~rexpr => BinaryConditionalExpr(op, lexpr, rexpr)
  }

  def node_trans = "." ~> non_rel_expr_generator ^^ { NodeTransformationExpr(_) }

  def bind_trans = var_decls ^^ { BindTransformationExpr(_) }

  // generators
  def generator = (
    non_rel_expr_generator
    | rel_expr_generator
  )

  def non_rel_expr_generator = (
    boolean_generator
    | synset_generator
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
    "{}" ^^ { _ => AlgebraExpr(FetchOp.synsets) }
    | "{" ~> expr <~ "}" ^^ { SynsetByExprReq(_) }
  )

  def sense_generator = (
    "::" ^^ { _ => AlgebraExpr(FetchOp.senses) }
    | alphaLit ~ ":" ~ integerNum ~ ":" ~ alphaLit ^^ {
      case word~_~num~_~pos => SenseByWordFormAndSenseNumberAndPosReq(word, num, pos)
    }
    | alphaLit ~ ":" ~ integerNum ^^ {
      case word~_~num => AlgebraExpr(FetchOp.sensesByWordFormAndSenseNumber(word, num))
    }
  )

  def function_call_generator = notQuotedString ~ ("(" ~> expr <~ ")") ^^ { case name~y => FunctionExpr(name, y) }

  def quoted_word_generator = (
    backQuotedString ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
    | quotedString ^^ { value => AlgebraExpr(if (value === "") FetchOp.words else FetchOp.wordByValue(value)) }
    | doubleQuotedString ^^ { WordFormByRegexReq(_) }
  )

  def float_generator = floatNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def sequence_generator = integerNum ~ ".." ~ integerNum ^^ { case left~_~right => AlgebraExpr(ConstantOp(DataSet.fromList((left to right).toList))) }
  def integer_generator = integerNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def back_generator = "#" ^^ { x => ContextReferenceReq() }
  def filter_generator = filter ^^ { BooleanByFilterReq(_) }
  def empty_set_generator = "<>" ^^ { _ => AlgebraExpr(ConstantOp.empty) }
  def expr_generator = "(" ~> expr <~ ")"
  def variable_generator = var_decl ^^ { ContextByVariableReq(_) }
  def arc_generator = "\\" ~> arc_expr ^^ { ArcByArcExprReq(_) }

  // variables
  def var_decls = rep1(var_decl) ^^ { VariableTemplate(_) }

  def var_decl = (
    step_var_decl
    | path_var_decl
  )

  def step_var_decl = "$" ~> notQuotedString ^^ { StepVariable(_) }
  def path_var_decl = "@" ~> notQuotedString ^^ { PathVariable(_) }

  // literals
  def alphaLit = (
    backQuotedString
    | quotedString
    | notQuotedString
  )

  def floatNum: Parser[Double] = "([0-9]+(([eE][+-]?[0-9]+)|\\.(([0-9]+[eE][+-]?[0-9]+)|([eE][+-]?[0-9]+)|([0-9]+))))|(\\.[0-9]+([eE][+-]?[0-9]+)?)".r ^^ { _.toDouble }
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
