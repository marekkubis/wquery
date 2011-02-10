package org.wquery.parser
import org.wquery.{WQueryParsingErrorException, WQueryParsingFailureException}
import org.wquery.engine.{EvaluableExpr, FunctionExpr, WQueryFunctions, ImperativeExpr, IteratorExpr, EmissionExpr, EvaluableAssignmentExpr, RelationalAssignmentExpr, IfElseExpr, BinaryPathExpr, BlockExpr, BinaryArithmExpr, TransformationDesc, StepExpr, RelationalExpr, QuantifierLit, FilterTransformationDesc, OrExpr, NotExpr, AndExpr, ComparisonExpr, BooleanLit, SynsetAllReq, SenseAllReq, DoubleQuotedLit, WordFormByRegexReq, ContextByRelationalExprReq, IntegerLit, ContextByReferenceReq, FloatLit, NotQuotedIdentifierLit, QuotedIdentifierLit, StringLit, ContextByVariableReq, BooleanByFilterReq, SequenceLit, SynsetByExprReq, SenseByWordFormAndSenseNumberAndPosReq, SenseByWordFormAndSenseNumberReq, UnaryRelationalExpr, InvertedRelationalExpr, QuantifiedRelationalExpr, RelationTransformationDesc, PathExpr, MinusExpr, UnionRelationalExpr}
import scala.util.parsing.combinator.RegexParsers

trait WQueryParsers extends RegexParsers {

  def parse(input: String): EvaluableExpr = {
    parse(query, input) match {
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
      "do" ~> rep(statement) <~ "end" ^^ { x => BlockExpr(x) }
      | statement  
  )  
  
  def statement = (
      iterator
      | emission
      | assignment
      | ifelse  
  )  
  
  def iterator = "from" ~ var_decls ~ "in" ~ multipath_expr ~ imp_expr ^^ {
    case _~vdecls~_~mexpr~iexpr => IteratorExpr(vdecls, mexpr, iexpr) 
  }     
  
  def emission = "emit" ~> multipath_expr ^^ { x => EmissionExpr(x) }
  
  def assignment = (
      var_decls ~ "=" ~ multipath_expr ^^ { case vdecls~_~mexpr => EvaluableAssignmentExpr(vdecls, mexpr) }
      | notQuotedId ~ "=" ~ rel_expr  ^^ { case name~_~rexpr => RelationalAssignmentExpr(name, rexpr) }
      // | expr ~ ("+="|"-="|"=") ~ expr ^^ { case lexpr~op~rexpr => UpdateExpr(lexpr, op, rexpr) }
  )
  
  def ifelse = "if" ~> multipath_expr ~ imp_expr ~ opt("else" ~> imp_expr) ^^ {
    case cexpr ~ iexpr ~ eexpr => IfElseExpr(cexpr, iexpr, eexpr)
  }
  
  def var_decls = repsep(var_decl, ",")
  
  def var_decl = "$" ~> notQuotedId
  
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
    = chainl1(unary_expr, ("*"|"/"|"%") ^^ { x => ((l:EvaluableExpr, r:EvaluableExpr) => BinaryArithmExpr(x, l , r)) })
  
  def unary_expr = (
      "-" ~> func_expr ^^ { x => MinusExpr(x) }
      | "+" ~> func_expr
      | func_expr
  )    
  
  def func_expr = (
      notQuotedId ~ ("(" ~> expr <~ ")") ^^ { case name~y => FunctionExpr(name, y) }
      | path  
  )
    
  // paths
  
  def path 
    = chainl1(generator, step, success((l:EvaluableExpr, r:TransformationDesc) => StepExpr(l , r))) ^^ { x => PathExpr(x) }
    
  def step = (
      relational_trans
      | filter_trans
  )
  
  def relational_trans = dots ~ rel_expr ^^ {
    case pos~expr => RelationTransformationDesc(pos, expr)
  }
  
  def dots = rep1(".") ^^ { x => x.size }    
  
  def rel_expr: Parser[RelationalExpr]
    = chainl1(quant_rel_expr, 
               "|" ^^ { x => ((l:RelationalExpr, r:RelationalExpr) => UnionRelationalExpr(l, r)) })
        
  def quant_rel_expr = unary_rel_expr ~ quantifierLit ^^ { case iexpr~quant => QuantifiedRelationalExpr(iexpr, quant) }
    
  def unary_rel_expr = (
      "(" ~> rel_expr <~ ")"
      | opt("^") ~ idLit ^^ {
        case Some(_)~id =>
          InvertedRelationalExpr(UnaryRelationalExpr(id))
        case None~id =>
          UnaryRelationalExpr(id)      
      }
  )    

  def quantifierLit = (
      "!" ^^^ {QuantifierLit(1, None)}      
      |success(QuantifierLit(1, Some(1)))      
  )
    
  def filter_trans = filter ^^ { x => FilterTransformationDesc(x) }
  
  def filter = "[" ~> or_condition <~ "]"
  
  def or_condition: Parser[OrExpr] = repsep(and_condition, "or") ^^ { x => OrExpr(x) }  
  
  def and_condition = repsep(not_condition, "and") ^^ { x => AndExpr(x) }
  
  def not_condition = (
      "not" ~> condition ^^ { x => NotExpr(x) }
      | condition
  )
  
  def condition = (
      "(" ~> or_condition <~ ")"
      | comparison
  // | boolean path
  )
  
  def comparison = expr ~ ("<="|"<"|">="|">"|"=~"|"="|"!="|"in"|"pin") ~ expr ^^ {
    case lexpr~op~rexpr => ComparisonExpr(op, lexpr, rexpr)
  }  
  
  // generators
  def generator = (
      boolean_generator
      |synset_generator
      |sense_generator
      |word_generator
      |float_generator
      |sequence_generator
      |integer_generator
      |back_generator  
      |filter_generator
      |expr_generator
      |variable_generator    
  )

  def boolean_generator = (
      "true" ^^ { x => BooleanLit(true) }
      | "false" ^^ { x => BooleanLit(false) }
  ) 

  def synset_generator = (
      "{}" ^^ { x => SynsetAllReq() }
      | "{" ~> expr <~ "}" ^^ { x => SynsetByExprReq(x) }
  )
  
  def sense_generator = (
      "::" ^^ { x => SenseAllReq() }
      | alphaLit ~ ":" ~ integerLit ~ ":" ~ alphaLit ^^ {
        case StringLit(word)~_~IntegerLit(num)~_~StringLit(pos) => SenseByWordFormAndSenseNumberAndPosReq(word, num, pos) 
      }   
      | alphaLit ~ ":" ~ integerLit ^^ {
        case StringLit(word)~_~IntegerLit(num) => SenseByWordFormAndSenseNumberReq(word, num)
      } 
  )
  
  def word_generator = (
      stringLit
      | doubleQuotedLit ^^ { case DoubleQuotedLit(x) => WordFormByRegexReq(x) }      
      | rel_expr ^^ { x => ContextByRelationalExprReq(x) }        
  )  

  def float_generator = floatLit  
  def sequence_generator = integerLit ~ ".." ~ integerLit ^^ { case IntegerLit(l)~_~IntegerLit(r) => SequenceLit(l, r) }  
  def integer_generator = integerLit  
  def back_generator = rep1("#") ^^ { x => ContextByReferenceReq(x.size) }
  def filter_generator = filter ^^ { x => BooleanByFilterReq(x) }
  def expr_generator = "(" ~> expr <~ ")"
  def variable_generator = var_decl ^^ { x => ContextByVariableReq(x) }
  
  // literals
  def alphaLit = stringLit|idLit ^^ { 
      case NotQuotedIdentifierLit(x) => StringLit(x)
      case QuotedIdentifierLit(x) => StringLit(x)
  }
  
  def floatLit: Parser[FloatLit] = "([0-9]+(([eE][+-]?[0-9]+)|\\.(([0-9]+[eE][+-]?[0-9]+)|([eE][+-]?[0-9]+)|([0-9]+))))|(\\.[0-9]+([eE][+-]?[0-9]+)?)".r ^^ { x => FloatLit(x.toDouble) }  
  def integerLit: Parser[IntegerLit] = "[0-9]+".r ^^ { x => IntegerLit(x.toInt) }
  
  def doubleQuotedLit: Parser[DoubleQuotedLit] = "\"(\\\\\"|[^\"])*?\"".r ^^ { x => 
    DoubleQuotedLit(x.substring(1, x.length - 1).replaceAll("\\\"","\"")) 
  }
  
  def stringLit: Parser[StringLit] = "`(\\\\`|[^`])*?`".r ^^ { x =>
    StringLit(x.substring(1, x.length - 1).replaceAll("\\`","`"))
  }  

  def idLit = (
      notQuotedId ^^ { x => NotQuotedIdentifierLit(x) }
      | quotedId ^^ { x => QuotedIdentifierLit(x) }
  )    
  
  def quotedId: Parser[String] = "'(\\\\'|[^'])*?'".r ^^ { x => x.substring(1, x.length - 1).replaceAll("\\'","'") }
  
  def notQuotedId: Parser[String] = new Parser[String] {
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
