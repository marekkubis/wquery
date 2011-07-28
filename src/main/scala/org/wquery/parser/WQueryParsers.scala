package org.wquery.parser
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import scala.util.parsing.combinator.RegexParsers
import org.wquery.model.DataSet
import org.wquery.engine._

trait WQueryParsers extends RegexParsers {

  def parse(input: String): EvaluableExpr = {
    parseAll(query, input) match {
      case Success(expr, _) =>
        expr
      case Failure(message, _) =>
        throw new WQueryParsingFailureException(message)
      case Error(message, _) =>
        throw new WQueryParsingErrorException(message)
    }
  }

  // parsers

  def query = (
    imp_expr
    | multipath_expr ^^ { x => FunctionExpr(WQueryFunctions.Sort, FunctionExpr(WQueryFunctions.Distinct, x)) }
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
    | assignment
    | ifelse
    | whiledo
  )

  def iterator = "from" ~> multipath_expr ~ imp_expr ^^ { case mexpr~iexpr => IteratorExpr(mexpr, iexpr) }

  def emission = "emit" ~> multipath_expr ^^ { EmissionExpr(_) }

  def assignment = (
     var_decls ~ ":=" ~ multipath_expr ^^ { case vdecls~_~mexpr => AssignmentExpr(vdecls, mexpr) }
     | notQuotedString ~ ":=" ~ arc_expr_union  ^^ { case name~_~rexpr => RelationalAliasExpr(name, rexpr) }
      // | expr ~ ("+="|"-="|":=") ~ expr ^^ { case lexpr~op~rexpr => UpdateExpr(lexpr, op, rexpr) }
  )

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

  def path = generator ~ rep(step) ^^ { case gen~steps => PathExpr(gen, steps) }

  def step = (
    node_trans
    | relation_trans
    | filter_trans
    | projection_trans
    | bind_trans
  )

  def relation_trans = positioned_relation_chain_trans ~ quantifier ^^ { case trans~quant => QuantifiedTransformationExpr(trans, quant) }

  def positioned_relation_chain_trans = (
    "(" ~> rep1(positioned_relation_trans) <~ ")" ^^ { PositionedRelationChainTransformationExpr(_) }
    | positioned_relation_trans ^^ { trans => PositionedRelationChainTransformationExpr(List(trans)) }
  )

  def positioned_relation_trans = dots ~ arc_expr_union ^^ { case pos~expr => PositionedRelationTransformationExpr(pos, expr) }

  def dots = rep1(".") ^^ { _.size }

  def arc_expr_union = rep1sep(arc_expr, "|")  ^^ { ArcExprUnion(_) }

  def arc_expr = (
    ("^" ~> notQuotedString) ^^ { id => ArcExpr(List("destination", id, "source")) } // syntactic sugar
    | rep1sep(notQuotedString, "^") ^^ { ArcExpr(_) }
  )

  def quantifier = (
    ("!"|"+") ^^^ { Quantifier(1, None) }
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

  def or_condition: Parser[OrExpr] = repsep(and_condition, "or") ^^ { OrExpr(_) }

  def and_condition = repsep(not_condition, "and") ^^ { AndExpr(_) }

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
    case lexpr~op~rexpr => ComparisonExpr(op, lexpr, rexpr)
  }

  def node_trans = "." ~> non_rel_expr_generator ^^ { NodeTransformationExpr(_) }

  def projection_trans = projection  ^^ { ProjectionTransformationExpr(_) }

  def projection = "<" ~> expr <~ ">"

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

  def rel_expr_generator = arc_expr_union ~ quantifier ^^ { case expr~quant => ContextByArcExprUnionReq(expr, quant) }

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
      case word~_~num~_~pos => AlgebraExpr(FetchOp.senseByWordFormAndSenseNumberAndPos(word, num, pos))
    }
    | alphaLit ~ ":" ~ integerNum ^^ {
      case word~_~num => AlgebraExpr(FetchOp.sensesByWordFormAndSenseNumber(word, num))
    }
  )

  def function_call_generator = notQuotedString ~ ("(" ~> expr <~ ")") ^^ { case name~y => FunctionExpr(name, y) }

  def quoted_word_generator = (
    backQuotedString ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
    | quotedString ^^ { value => AlgebraExpr(if (value == "") FetchOp.words else FetchOp.wordByValue(value)) }
    | doubleQuotedString ^^ { WordFormByRegexReq(_) }
  )

  def float_generator = floatNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def sequence_generator = integerNum ~ ".." ~ integerNum ^^ { case left~_~right => AlgebraExpr(ConstantOp(DataSet.fromList((left to right).toList))) }
  def integer_generator = integerNum ^^ { value => AlgebraExpr(ConstantOp.fromValue(value)) }
  def back_generator = rep1("#") ^^ { x => ContextReferenceReq(x.size - 1) }
  def filter_generator = filter ^^ { BooleanByFilterReq(_) }
  def empty_set_generator = "<>" ^^ { _ => AlgebraExpr(ConstantOp.empty) }
  def expr_generator = "(" ~> expr <~ ")"
  def variable_generator = var_decl ^^ { ContextByVariableReq(_) }
  def arc_generator = "\\" ~> arc_expr ^^ { ArcByArcExprReq(_) }

  // variables
  def var_decls = rep1(var_decl)

  def var_decl = (
    "$" ~> notQuotedString ^^ { StepVariable(_) }
    | "@" ~> notQuotedString ^^ { PathVariable(_) }
  )

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
    def apply(in: Input) = {
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
