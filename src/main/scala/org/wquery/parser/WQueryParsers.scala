package org.wquery.parser
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import org.wquery.engine.{EvaluableExpr, FunctionExpr, WQueryFunctions, ImperativeExpr, IteratorExpr, EmissionExpr, EvaluableAssignmentExpr, RelationalAssignmentExpr, IfElseExpr, BlockExpr, TransformationExpr, RelationalExpr, OrExpr, NotExpr, AndExpr, ComparisonExpr, SynsetAllReq, WordFormByRegexReq, ContextByRelationalExprReq, ContextByReferenceReq, ContextByVariableReq, BooleanByFilterReq, SynsetByExprReq, UnaryRelationalExpr, QuantifiedRelationalExpr, UnionRelationalExpr, WhileDoExpr, FilterTransformationExpr, ProjectionTransformationExpr, BindTransformationExpr, RelationTransformationExpr, ArcByUnaryRelationalExprReq, FloatByValueReq, IntegerByValueReq, BinaryArithmExpr, BinaryPathExpr, StepExpr, SenseAllReq, SenseByWordFormAndSenseNumberAndPosReq, SenseByWordFormAndSenseNumberReq, MinusExpr, PathExpr, StringByValueReq, SequenceByBoundsReq, BooleanByValueReq, Quantifier, WordFormByValueReq, StepVariable, PathVariable, PathConditionExpr}
import scala.util.parsing.combinator.RegexParsers

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

  def imp_expr: Parser[ImperativeExpr] = (
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
      var_decls ~ ":=" ~ multipath_expr ^^ { case vdecls~_~mexpr => EvaluableAssignmentExpr(vdecls, mexpr) }
      | notQuotedString ~ ":=" ~ rel_expr  ^^ { case name~_~rexpr => RelationalAssignmentExpr(name, rexpr) }
      // | expr ~ ("+="|"-="|":=") ~ expr ^^ { case lexpr~op~rexpr => UpdateExpr(lexpr, op, rexpr) }
  )

  def ifelse = "if" ~> multipath_expr ~ imp_expr ~ opt("else" ~> imp_expr) ^^ {
    case cexpr ~ iexpr ~ eexpr => IfElseExpr(cexpr, iexpr, eexpr)
  }

  def whiledo = "while" ~> multipath_expr ~ imp_expr ^^ { case cexpr~iexpr => WhileDoExpr(cexpr, iexpr) }

  // mutlipath exprs

  def multipath_expr: Parser[EvaluableExpr]
    = chainl1(intersect_expr,
              ("union"|"except") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryPathExpr(x, l , r)) })

  def intersect_expr
    = chainl1(with_expr,
               "intersect" ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryPathExpr(x, l , r)) })

  def with_expr
    = chainl1(add_expr, "," ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryPathExpr(x, l , r)) })

  // arithmetics

  def add_expr
    = chainl1(mul_expr, ("+"|"-") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmExpr(x, l , r)) })

  def mul_expr
    = chainl1(unary_expr, ("*"|"/"|"%"|"div") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmExpr(x, l , r)) })

  def unary_expr = (
      "-" ~> path ^^ { MinusExpr(_) }
      | "+" ~> path
      | path
  )

  def path = chainl1(generator, step, success((l:EvaluableExpr, r:TransformationExpr) => StepExpr(l , r))) ^^ { x => PathExpr(x) }

  def step = (
      generator_trans
      | relation_trans
      | filter_trans
      | projection_trans
      | bind_trans
  )

  def relation_trans = dots ~ rel_expr ^^ { case pos~expr => RelationTransformationExpr(pos, expr) }

  def dots = rep1(".") ^^ { _.size }

  def rel_expr: Parser[RelationalExpr]
    = chainl1(quant_rel_expr, "|" ^^ { x => ((l:RelationalExpr, r:RelationalExpr) => UnionRelationalExpr(l, r)) })

  def quant_rel_expr = unary_rel_expr ~ quantifier ^^ { case iexpr~quant => QuantifiedRelationalExpr(iexpr, quant) }

  def unary_rel_expr = (
      ("^" ~> notQuotedString) ^^ { id => UnaryRelationalExpr(List("destination", id, "source")) } // syntactic sugar
      | rep1sep(notQuotedString, "^") ^^ { UnaryRelationalExpr(_) }
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

  def generator_trans = "." ~> non_rel_expr_generator ^^ { gen => FilterTransformationExpr(ComparisonExpr("in", ContextByReferenceReq(1), gen)) }

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
      | expr_generator
      | variable_generator
      | arc_generator
      | quoted_word_generator
  )

  def rel_expr_generator = rel_expr ^^ { ContextByRelationalExprReq(_) }

  def boolean_generator = (
      "true" ^^^ { BooleanByValueReq(true) }
      | "false" ^^^ { BooleanByValueReq(false) }
  )

  def synset_generator = (
      "{}" ^^ { _ => SynsetAllReq() }
      | "{" ~> expr <~ "}" ^^ { SynsetByExprReq(_) }
  )

  def sense_generator = (
      "::" ^^ { _ => SenseAllReq() }
      | alphaLit ~ ":" ~ integerNum ~ ":" ~ alphaLit ^^ {
        case word~_~num~_~pos => SenseByWordFormAndSenseNumberAndPosReq(word, num, pos)
      }
      | alphaLit ~ ":" ~ integerNum ^^ {
        case word~_~num => SenseByWordFormAndSenseNumberReq(word, num)
      }
  )

  def function_call_generator = notQuotedString ~ ("(" ~> expr <~ ")") ^^ { case name~y => FunctionExpr(name, y) }

  def quoted_word_generator = (
      backQuotedString ^^ { StringByValueReq(_) }
      | quotedString ^^ { WordFormByValueReq(_) }
      | doubleQuotedString ^^ { WordFormByRegexReq(_) }
  )

  def float_generator = floatNum ^^ { FloatByValueReq(_) }
  def sequence_generator = integerNum ~ ".." ~ integerNum ^^ { case l~_~r => SequenceByBoundsReq(l, r) }
  def integer_generator = integerNum ^^ { IntegerByValueReq(_) }
  def back_generator = rep1("#") ^^ { x => ContextByReferenceReq(x.size) }
  def filter_generator = filter ^^ { BooleanByFilterReq(_) }
  def expr_generator = "(" ~> expr <~ ")"
  def variable_generator = var_decl ^^ { ContextByVariableReq(_) }
  def arc_generator = "\\" ~> unary_rel_expr ^^ { ArcByUnaryRelationalExprReq(_) }

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
