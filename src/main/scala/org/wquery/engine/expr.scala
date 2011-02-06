package org.wquery.engine
import org.wquery.WQueryEvaluationException
import org.wquery.model.{ AggregateFunction, ScalarFunction, WordNet, FloatType, IntegerType, UnionType, BasicDataType, ValueType, TupleType, StringType, Relation, SynsetType, SenseType, Sense }
import scala.collection.mutable.ListBuffer

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context): DataSet
}

sealed abstract class ContextFreeExpr extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings): DataSet

  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context): DataSet = evaluate(functions, wordNet, bindings)
}

sealed abstract class BindingsFreeExpr extends ContextFreeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet): DataSet

  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings): DataSet = evaluate(functions, wordNet)
}

sealed abstract class FunctionsFreeExpr extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet): DataSet

  def evaluate(functions: FunctionSet, wordNet: WordNet): DataSet = evaluate(wordNet)
}

sealed abstract class SelfEvaluableExpr extends FunctionsFreeExpr {
  def evaluate(): DataSet

  def evaluate(wordNet: WordNet): DataSet = evaluate
}

/*
 * Selectors <...> releated expressions 
 */
sealed abstract class SelectorExpr(expr: Expr) extends Expr {
  def argument = expr

  def selected: Boolean
}

case class SelectExpr(expr: Expr) extends SelectorExpr(expr) {
  def selected = true
}

case class SkipExpr(expr: Expr) extends SelectorExpr(expr) {
  def selected = false
}

/*
 * Imperative expressions
 */

sealed abstract class ImperativeExpr extends EvaluableExpr

case class EmissionExpr(expr: EvaluableExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    expr.evaluate(functions, wordNet, bindings, context)
  }
}

case class IteratorExpr(vars: List[String], mexpr: EvaluableExpr, iexpr: ImperativeExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val mresult = mexpr.evaluate(functions, wordNet, bindings, context)
    val tupleBindings = new Bindings(Some(bindings))
    val buffer = new DataSetBuffer

    if (mresult.types.size < vars.size) {
      throw new WQueryEvaluationException("A tuple generated by a path expression in an iterator contains" +
        " an insufficient number of elements to bind all variables")
    }

    for (tuple <- mresult.content) {
      for (i <- 0 until vars.size) {
        tupleBindings.bind(vars(i), tuple(i))
      }

      buffer.append(iexpr.evaluate(functions, wordNet, tupleBindings, context))
    }

    buffer.toDataSet
  }
}

case class IfElseExpr(cexpr: EvaluableExpr, iexpr: ImperativeExpr, eexpr: Option[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val cresult = cexpr.evaluate(functions, wordNet, bindings, context)

    if (cresult.isTrue) {
      iexpr.evaluate(functions, wordNet, bindings, context)
    } else {
      eexpr match {
        case Some(eexpr) =>
          eexpr.evaluate(functions, wordNet, bindings, context)
        case None =>
          DataSet.empty
      }
    }
  }
}

case class BlockExpr(exprs: List[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val blockBindings = new Bindings(Some(bindings))
    val buffer = new DataSetBuffer

    for (expr <- exprs) {
      buffer.append(expr.evaluate(functions, wordNet, blockBindings, context))
    }

    buffer.toDataSet
  }
}

case class EvaluableAssignmentExpr(vars: List[String], expr: EvaluableExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val eresult = expr.evaluate(functions, wordNet, bindings, context)

    if (eresult.content.size == 1) {
      val tuple = eresult.content.head

      if (vars.size == tuple.size) {
        for (i <- 0 until vars.size) {
          bindings.bind(vars(i), tuple(i))
        }

        DataSet.empty
      } else {
        throw new WQueryEvaluationException("A tuple generated by a path expression in an assignment contains" +
          " an insufficient number of elements to bind all variables")
      }

      DataSet.empty
    } else {
      throw new WQueryEvaluationException("A multipath expression in an assignment should contain exactly one tuple")
    }
  }
}

case class RelationalAssignmentExpr(name: String, rexpr: RelationalExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    bindings.bind(name, rexpr)
    DataSet.empty
  }
}

/*
 * Multipath expressions
 */
case class BinaryPathExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val leval = left.evaluate(functions, wordNet, bindings, context)
    val reval = right.evaluate(functions, wordNet, bindings, context)

    op match {
      case "," =>
        DataSet(leval.types ++ reval.types, join(leval.content, reval.content))
      case op =>
        if (leval.types != reval.types)
          throw new WQueryEvaluationException("Arguments of 'union' have different types")

        DataSet(
          leval.types,
          op match {
            case "union" =>
              leval.content union reval.content
            case "except" =>
              leval.content.filterNot(reval.content.contains)
            case "intersect" =>
              leval.content intersect reval.content
            case _ =>
              throw new IllegalArgumentException("Unknown binary path operator '" + op + "'")
          })
    }
  }

  def join(left: List[List[Any]], right: List[List[Any]]) = {
    val buffer = new ListBuffer[List[Any]]

    for (ltuple <- left) {
      for (rtuple <- right) {
        buffer append (ltuple ++ rtuple)
      }
    }

    buffer.toList
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val lresult = left.evaluate(functions, wordNet, bindings, context)
    val rresult = right.evaluate(functions, wordNet, bindings, context)

    if (lresult.types.size == 1 && rresult.types.size == 1) {
      if (lresult.types.head == FloatType) {
        if (rresult.types.head == FloatType) {
          DataSet(List(FloatType), combineUsingDoubleArithmOp(op, lresult.content, rresult.content))
        } else {
          DataSet(List(FloatType),
            combineUsingDoubleArithmOp(op,
              lresult.content,
              rresult.content map { case List(x: Int) => List(x.doubleValue) }))
        }
      } else if (rresult.types.head == FloatType) {
        DataSet(List(FloatType),
          combineUsingDoubleArithmOp(op,
            lresult.content map { case List(x: Int) => List(x.doubleValue) },
            rresult.content))
      } else {
        DataSet(List(IntegerType), combineUsingIntArithmOp(op, lresult.content, rresult.content))
      }
    } else {
      throw new WQueryEvaluationException("'" + op + "' requires arguments that contain only singleton tuples")
    }
  }

  def combineUsingDoubleArithmOp(op: String, leval: List[List[Any]], reval: List[List[Any]]) = {
    op match {
      case "+" =>
        combine[Double]((x, y) => x + y, leval, reval)
      case "-" =>
        combine[Double]((x, y) => x - y, leval, reval)
      case "*" =>
        combine[Double]((x, y) => x * y, leval, reval)
      case "/" =>
        combine[Double]((x, y) => x / y, leval, reval)
      case "%" =>
        combine[Double]((x, y) => x % y, leval, reval)
      case _ =>
        throw new IllegalArgumentException("Unknown binary arithmetic operator '" + op + "'")
    }
  }

  def combineUsingIntArithmOp(op: String, leval: List[List[Any]], reval: List[List[Any]]) = {
    op match {
      case "+" =>
        combine[Int]((x, y) => x + y, leval, reval)
      case "-" =>
        combine[Int]((x, y) => x - y, leval, reval)
      case "*" =>
        combine[Int]((x, y) => x * y, leval, reval)
      case "/" =>
        combine[Int]((x, y) => x / y, leval, reval)
      case "%" =>
        combine[Int]((x, y) => x % y, leval, reval)
      case _ =>
        throw new IllegalArgumentException("Unknown binary arithmetic operator '" + op + "'")
    }
  }

  def combine[A](func: (A, A) => A, left: List[List[Any]], right: List[List[Any]]) = {
    val buffer = new ListBuffer[List[Any]]

    for (ltuple <- left) {
      for (rtuple <- right) {
        (ltuple, rtuple) match {
          case (List(lval), List(rval)) =>
            buffer append List(func(lval.asInstanceOf[A], rval.asInstanceOf[A]))
          case _ =>
            throw new WQueryEvaluationException("'" + op + "' requires singleton tuples as arguments")
        }
      }
    }

    buffer.toList
  }
}

case class MinusExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val eresult = expr.evaluate(functions, wordNet, bindings, context)

    eresult.types match {
      case List(FloatType) =>
        DataSet.fromList(FloatType, eresult.content.map {
          case List(value: Double) =>
            -value
          case tuple =>
            throw new WQueryEvaluationException("Tuple " + tuple + " contains a value forbidden in float context")
        })
      case List(IntegerType) =>
        DataSet.fromList(IntegerType, eresult.content.map {
          case List(value: Int) =>
            -value
          case tuple =>
            throw new WQueryEvaluationException("Tuple " + tuple + " contains a value forbidden in integer context")
        })
      case _ =>
        throw new WQueryEvaluationException("Unary '-' requires a context that consist of integer or float singletons")
    }
  }
}

/* 
 * Function call expressions
 */
case class FunctionExpr(name: String, args: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val aresult = args.evaluate(functions, wordNet, bindings, context)

    val atypes = aresult.types.map {
      case UnionType(utypes) =>
        if (utypes == Set(FloatType, IntegerType)) ValueType(FloatType) else TupleType
      case t: BasicDataType =>
        ValueType(t)
    }

    val (func, method) = functions.getFunction(name, atypes) match {
      case Some(f) => f
      case None =>
        // promote arg types from integers to floats
        functions.getFunction(
          name,
          atypes.map {
            case ValueType(IntegerType) =>
              ValueType(FloatType)
            case t => t
          }) match {
          case Some(f) => f
          case None =>
            functions.demandFunction(name, List(TupleType))
        }
    }

    func match {
      case func: AggregateFunction =>
        method.invoke(WQueryFunctions, aresult).asInstanceOf[DataSet]
      case func: ScalarFunction =>
        val buffer = new ListBuffer[List[Any]]()
        val margs = new Array[AnyRef](aresult.types.size)
        var stop = false

        for (tuple <- aresult.content) {
          for (i <- 0 until margs.size)
            margs(i) = tuple(i).asInstanceOf[AnyRef]

          buffer.append(List(method.invoke(WQueryFunctions, margs: _*)))
        }

        func.result match {
          case ValueType(dtype) =>
            DataSet(List(dtype), buffer.toList)
          case TupleType =>
            throw new RuntimeException("ScalarFunction '" + func.name + "' returned TupleType")
        }
    }
  }
}

/*
 * Path related expressions
 */
sealed abstract class SelectableExpr extends EvaluableExpr {
  def selected(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context): List[Boolean]
}

case class SelectableSelectorExpr(expr: SelectorExpr) extends SelectableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = expr argument match {
    case expr: EvaluableExpr =>
      expr.evaluate(functions, wordNet, bindings, context)
    case expr =>
      throw new RuntimeException("An attempt to evaluate non evaluable expression " + expr)
  }

  def selected(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    expr match {
      case SkipExpr(cexpr@ContextByRelationalExprReq(_)) =>
        List.fill(cexpr.stepCount(wordNet, bindings, context))(false)
      case SelectExpr(cexpr@ContextByRelationalExprReq(_)) =>
        List.fill(cexpr.stepCount(wordNet, bindings, context))(true)
      case _ =>
        List(expr.selected)
    }
  }
}

case class StepExpr(lexpr: SelectableExpr, rexpr: TransformationDesc) extends SelectableExpr {
  def selected(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    rexpr match {
      case RelationTransformationDesc(_, expr, _) =>
        val lresult = lexpr.evaluate(functions, wordNet, bindings, context)
        val rexpr = expr.argument.asInstanceOf[RelationalExpr]

        lexpr.selected(functions, wordNet, bindings, context) ++ List(expr.selected)
      case FilterTransformationDesc(expr) =>
        val cond = expr.argument.asInstanceOf[ConditionalExpr]
        val lresult = lexpr.evaluate(functions, wordNet, bindings, context)

        lexpr.selected(functions, wordNet, bindings, context) ++ List(expr.selected)
    }
  }

  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    rexpr match {
      case RelationTransformationDesc(pos, expr, quantifier) =>
        val lresult = lexpr.evaluate(functions, wordNet, bindings, context)
        val rexpr = expr.argument.asInstanceOf[RelationalExpr]

        quantifier match {
          case QuantifierLit(1, Some(1)) =>
            rexpr.transform(wordNet, bindings, context, lresult, pos, false, false)
          case QuantifierLit(1, None) =>
            val head = rexpr.transform(wordNet, bindings, context, lresult, pos, false, true)
            val content = new ListBuffer[List[Any]]

            for (tuple <- head.content) {
              content.append(tuple)
              val prefix = tuple.slice(0, tuple.size - 1) // remove prefix variable put tuple to outer closure

              for (cnode <- outerclosure(wordNet, bindings, context, rexpr, false, List(tuple.last), Set(tuple.last))) {
                content.append(prefix ++ List(cnode))
              }
            }

            DataSet(head.types, content.toList)
          case _ =>
            throw new IllegalArgumentException("Unsupported quantifier value " + quantifier)
        }

      case FilterTransformationDesc(expr) =>
        val cond = expr.argument.asInstanceOf[ConditionalExpr]
        val lresult = lexpr.evaluate(functions, wordNet, bindings, context)

        DataSet(
          lresult.types ++ List(lresult.types.last),
          lresult.content
          .filter(tuple => cond.satisfied(functions, wordNet, bindings, Context(lresult.types, tuple)))
          .map(x => x ::: List(x.last)))
    }
  }

  private def outerclosure(wordNet: WordNet, bindings: Bindings, context: Context, expr: RelationalExpr,
    inverted: Boolean, source: List[Any], forbidden: Set[Any]): List[List[Any]] = {
    val transformed = expr.transform(wordNet, bindings, context, DataSet.fromTuple(source), 1, inverted, true)
    val filtered = transformed.content.filter { x => !forbidden.contains(x.last) }

    if (filtered.isEmpty) {
      filtered
    } else {
      val result = new ListBuffer[List[Any]]
      val newForbidden: Set[Any] = forbidden.++[Any, Set[Any]](filtered.map { x => x.last }) // TODO ugly

      result.appendAll(filtered)

      filtered.foreach { x =>
        result.appendAll(outerclosure(wordNet, bindings, context, expr, inverted, x, newForbidden))
      }

      result.toList
    }
  }
}

sealed abstract class TransformationDesc extends Expr

case class RelationTransformationDesc(pos: Int, expr: SelectorExpr, quant: QuantifierLit) extends TransformationDesc
case class FilterTransformationDesc(expr: SelectorExpr) extends TransformationDesc

case class PathExpr(expr: SelectableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    var selected = expr.selected(functions, wordNet, bindings, context)

    if (selected.forall(_ == false)) {
      selected = List.fill(selected.size - 1)(false) ++ List(true)
    }

    val eresult = expr.evaluate(functions, wordNet, bindings, context)
    val rtypes = eresult.types.zipWithIndex.filter { case (x, i) => selected(i) }.map { case (x, i) => x }

    val rbuffer = new ListBuffer[List[Any]]

    for (tuple <- eresult.content) {
      val tbuffer = new ListBuffer[Any]

      for (i <- (0 until tuple.size)) {
        if (selected(i))
          tuple(i) match {
            case list: List[_] =>
              tbuffer appendAll list
            case obj =>
              tbuffer append obj
          }
      }

      rbuffer append tbuffer.toList
    }

    DataSet(rtypes, rbuffer.toList)
  }
}

/*
 * Relational Expressions
 */
sealed abstract class RelationalExpr extends Expr {
  def transform(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean): DataSet

  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean): Int
}

case class UnaryRelationalExpr(identifier: IdentifierLit) extends RelationalExpr {
  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    identifier match {
      case QuotedIdentifierLit(_) =>
        1
      case NotQuotedIdentifierLit(id) =>
        if (pos == 0) { // we are in a generator 
          if (context.isEmpty) { // we are not in a filter
            1
          } else {
            useIdentifierAsVariable(id, wordNet, bindings, context, data, pos, invert, force) match {
              case Some(_) =>
                2
              case None =>
                if (wordNet.containsRelation(id, data.types(data.types.size - 1))) 2 else 1
            }
          }
        } else {
          1
        }
    }
  }

  def transform(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    if (force) {
      useIdentifierAsTransformation(identifier.value, wordNet, data, pos, invert)
    } else {
      identifier match {
        case QuotedIdentifierLit(wordForm) =>
          if (pos == 0) {
            useIdentifierAsGenerator(wordForm, wordNet, context, invert)
          } else {
            throw new WQueryEvaluationException("Quoted identifier found after '.'")
          }
        case NotQuotedIdentifierLit(relname) =>
          if (pos == 0) { // we are in a generator 
            if (context.isEmpty) { // we are not in a filter      
              useIdentifierAsGenerator(relname, wordNet, context, invert)
            } else {
              useIdentifierAsVariable(relname, wordNet, bindings, context, data, pos, invert, force) match {
                case Some(result) =>
                  result
                case None =>
                  if (wordNet.containsRelation(relname, data.types(data.types.size - 1))) {
                    useIdentifierAsTransformation(relname, wordNet, data, 1, invert)
                  } else {
                    useIdentifierAsGenerator(relname, wordNet, context, invert)
                  }
              }
            }
          } else {
            useIdentifierAsVariable(relname, wordNet, bindings, context, data, pos, invert, force) match {
              case Some(result) =>
                result
              case None =>
                useIdentifierAsTransformation(relname, wordNet, data, pos, invert)
            }
          }
      }
    }
  }

  private def useIdentifierAsVariable(id: String, wordNet: WordNet, bindings: Bindings, context: Context,
    data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    bindings.lookup(id) match {
      case Some(rexpr: RelationalExpr) =>
        Some(rexpr.transform(wordNet, bindings, context, data, pos, invert, force))
      case _ =>
        None
    }
  }

  private def useIdentifierAsGenerator(id: String, wordNet: WordNet, context: Context, invert: Boolean) = {
    id match {
      case "" =>
        DataSet.fromList(StringType, wordNet.words.toList)
      case "false" =>
        DataSet.fromValue(false)
      case "true" =>
        DataSet.fromValue(true)
      case _ =>
        if (!invert) {
          DataSet.fromOptionalValue(wordNet.getWordForm(id), StringType)
        } else {
          if (context.isEmpty)
            throw new WQueryEvaluationException("'^' applied to the path generator")
          else
            throw new WQueryEvaluationException("Relation '" + id + "' not found")
        }
    }
  }

  private def useIdentifierAsTransformation(id: String, wordNet: WordNet, data: DataSet, pos: Int, invert: Boolean) = {
    val succfunc = if (invert) wordNet.getPredecessors(_, _, _) else wordNet.getSuccessors(_, _, _)
    val relation = wordNet.demandRelation(id, data.types(data.types.size - pos))
    val buffer = new ListBuffer[List[Any]]

    for (tuple <- data.content) {
      for (succ <- succfunc(tuple(tuple.size - pos), relation, Relation.Destination)) {
        buffer.append(tuple ::: List(succ))
      }
    }

    DataSet(data.types ++ List(relation.destinationType), buffer.toList)
  }
}

case class QuantifiedRelationalExpr(expr: RelationalExpr, quantifier: QuantifierLit) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    quantifier match {
      case QuantifierLit(1, Some(1)) =>
        expr.transform(wordNet, bindings, context, data, pos, invert, force)
      case QuantifierLit(1, None) =>
        val head = expr.transform(wordNet, bindings, context, data, pos, invert, force)
        val content = new ListBuffer[List[Any]]

        for (tuple <- head.content) {
          content.append(tuple)
          val prefix = tuple.slice(0, tuple.size - 1)

          for (cnode <- closure(wordNet, bindings, context, expr, invert, tuple.last, Set(tuple.last))) {
            content.append(prefix ++ List(cnode))
          }
        }

        DataSet(head.types, content.toList)
      case _ =>
        throw new IllegalArgumentException("Unsupported quantifier value " + quantifier)
    }
  }

  private def closure(wordNet: WordNet, bindings: Bindings, context: Context, expr: RelationalExpr,
    inverted: Boolean, source: Any, forbidden: Set[Any]): List[Any] = {
    val transformed = expr.transform(wordNet, bindings, context, DataSet.fromValue(source), 1, inverted, true)
    val filtered = transformed.content.filter { x => !forbidden.contains(x.last) }.map { x => x.last }

    if (filtered.isEmpty) {
      filtered
    } else {
      val result = new ListBuffer[Any]
      val newForbidden: Set[Any] = forbidden.++[Any, Set[Any]](filtered) //TODO ugly

      result.appendAll(filtered)
      filtered.foreach { x => result.appendAll(closure(wordNet, bindings, context, expr, inverted, x, newForbidden)) }
      result.toList
    }
  }

  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    expr.stepCount(wordNet, bindings, context, data, pos, invert, force)
  }
}

case class InvertedRelationalExpr(expr: RelationalExpr) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    expr.transform(wordNet, bindings, context, data, pos, !invert, force)
  }

  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    expr.stepCount(wordNet, bindings, context, data, pos, !invert, force)
  }
}

case class UnionRelationalExpr(lexpr: RelationalExpr, rexpr: RelationalExpr) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    val lresult = lexpr.transform(wordNet, bindings, context, data, pos, invert, force)
    val rresult = rexpr.transform(wordNet, bindings, context, data, pos, invert, force)

    if (lresult.types == rresult.types) {
      DataSet(lresult.types, lresult.content union rresult.content)
    } else {
      throw new WQueryEvaluationException("Unable to apply 'union' to relations that have different types")
    }
  }

  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context, data: DataSet, pos: Int, invert: Boolean, force: Boolean) = {
    val lcount = lexpr.stepCount(wordNet, bindings, context, data, pos, invert, force)
    val rcount = rexpr.stepCount(wordNet, bindings, context, data, pos, invert, force)

    if (lcount == rcount)
      lcount
    else
      throw new WQueryEvaluationException("'union' requires proper relation names on both sides")
  }
}

/*
 * Conditional Expressions
 */
sealed abstract class ConditionalExpr extends Expr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context): Boolean
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = exprs.exists { x => x.satisfied(functions, wordNet, bindings, context) }
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = exprs.forall { x => x.satisfied(functions, wordNet, bindings, context) }
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = !expr.satisfied(functions, wordNet, bindings, context)
}

case class ComparisonExpr(op: String, lexpr: EvaluableExpr, rexpr: EvaluableExpr) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val lresult = lexpr.evaluate(functions, wordNet, bindings, context)
    val rresult = rexpr.evaluate(functions, wordNet, bindings, context)

    op match {
      case "=" =>
        lresult == rresult
      case "!=" =>
        lresult != rresult
      case "in" =>
        lresult.types == rresult.types && lresult.content.forall(rresult.content.contains)
      case "pin" =>
        lresult.types == rresult.types &&
          lresult.content.forall(rresult.content.contains) &&
          lresult.content.size < rresult.content.size
      case "=~" =>
        if (rresult.content.size == 1) {
          // element context
          if (rresult.types.size == 1 && rresult.types.head == StringType) {
            val regex = rresult.content.head.head.asInstanceOf[String].r

            lresult.content.forall {
              case List(elem: String) =>
                regex findFirstIn (elem) match {
                  case Some(_) =>
                    true
                  case None =>
                    false
                }
              case _ =>
                false
            }
          } else {
            throw new WQueryEvaluationException("The right side of '" + op +
              "' should return exactly one character string value")
          }
        } else if (rresult.content.size == 0) {
          throw new WQueryEvaluationException("The right side of '" + op + "' returns no values")
        } else { // rresult.content.size > 0
          throw new WQueryEvaluationException("The right side of '" + op + "' returns more than one values")
        }
      case _ =>
        if (lresult.content.size == 1 && rresult.content.size == 1) {
          // element context
          op match {
            case "<=" =>
              WQueryFunctions.compare(lresult.content, rresult.content) <= 0
            case "<" =>
              WQueryFunctions.compare(lresult.content, rresult.content) < 0
            case ">=" =>
              WQueryFunctions.compare(lresult.content, rresult.content) >= 0
            case ">" =>
              WQueryFunctions.compare(lresult.content, rresult.content) > 0
            case _ =>
              throw new IllegalArgumentException("Unknown comparison operator '" + op + "'")
          }
        } else {
          if (lresult.content.size == 0)
            throw new WQueryEvaluationException("The left side of '" + op + "' returns no values")
          if (lresult.content.size > 1)
            throw new WQueryEvaluationException("The left side of '" + op + "' returns more than one value")
          if (rresult.content.size == 0)
            throw new WQueryEvaluationException("The right side of '" + op + "' returns no values")
          if (rresult.content.size > 1)
            throw new WQueryEvaluationException("The right side of '" + op + "' returns more than one values")

          // the following shall not happen
          throw new WQueryEvaluationException("Both sides of '" + op + "' should return exactly one value")
        }
    }
  }
}

/*
 * Generators
 */
case class SynsetAllReq() extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(SynsetType, wordNet.synsets.toList)
}

case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val eresult = expr.evaluate(functions, wordNet, bindings, context)

    DataSet.fromList(SynsetType, eresult types match {
      case List(SenseType) =>
        eresult.content.map {
          case List(sense: Sense) =>
            wordNet.getSynsetBySense(sense)
          case tuple =>
            throw new RuntimeException("Tuple " + tuple + " found in {...} instead of a sense")
        }
      case List(StringType) =>
        eresult.content.flatMap {
          case List(wordForm: String) =>
            wordNet.getSynsetsByWordForm(wordForm)
          case tuple =>
            throw new RuntimeException("Tuple " + tuple + " found in {...} instead of a word form")
        }
      case _ =>
        throw new WQueryEvaluationException("{...} requires an expression that generates senses or word forms")
    })
  }
}

case class SenseAllReq() extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(SenseType, wordNet.senses.toList)
}

case class SenseByWordFormAndSenseNumberAndPosReq(word: String, num: Int, pos: String) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromOptionalValue(wordNet.getSenseByWordFormAndSenseNumberAndPos(word, num, pos), SenseType)
}

case class SenseByWordFormAndSenseNumberReq(word: String, num: Int) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(SenseType, wordNet.getSensesByWordFormAndSenseNumber(word, num))
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    expr.transform(wordNet, bindings, context, DataSet(context.types, List(context.values)), 0, false, false)
  }

  def stepCount(wordNet: WordNet, bindings: Bindings, context: Context) = expr.stepCount(wordNet: WordNet, bindings: Bindings, context: Context,
    DataSet(context.types, List(context.values)), 0, false, false)
}

case class WordFormByRegexReq(v: String) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = {
    val regex = v.r
    val result = wordNet.words.filter { x =>
      regex findFirstIn (x) match {
        case Some(s) =>
          true
        case None =>
          false
      }
    }

    DataSet.fromList(StringType, result.toList)
  }
}

case class ContextByReferenceReq(ref: Int) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (context.size - ref >= 0) {
      val value = context.values(context.size - ref)
      DataSet.fromValue(if (value.isInstanceOf[List[_]]) value.asInstanceOf[List[_]].last else value)
    } else {
      throw new WQueryEvaluationException("Backward reference '" + List.fill(ref)("#").mkString +
        "' too far, the longest possible backward reference in this context is '" +
        List.fill(context.size)("#").mkString + "'")
    }
  }
}

case class ContextByVariableReq(variable: String) extends ContextFreeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    bindings.lookup(variable) match {
      case Some(value) =>
        DataSet.fromValue(value)
      case None =>
        throw new WQueryEvaluationException("A reference to unknown variable $" + variable + " found")
    }
  }
}

case class BooleanByFilterReq(cond: ConditionalExpr) extends ContextFreeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = DataSet.fromValue(cond.satisfied(functions, wordNet, bindings, Context.empty))
}

/*
 * Literals
 */
case class DoubleQuotedLit(v: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
}

case class StringLit(v: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
}

case class IntegerLit(v: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
}

case class SequenceLit(l: Int, r: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromList(IntegerType, (l to r).toList)
}

case class FloatLit(v: Double) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
}

case class BooleanLit(v: Boolean) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
}

case class QuantifierLit(l: Int, r: Option[Int]) extends Expr

sealed abstract class IdentifierLit(v: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(v)
  def value = v
}

case class NotQuotedIdentifierLit(val v: String) extends IdentifierLit(v)
case class QuotedIdentifierLit(val v: String) extends IdentifierLit(v)