package org.wquery.engine
import org.wquery.{WQueryEvaluationException, WQueryModelException}
import scala.collection.mutable.ListBuffer
import org.wquery.model._

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings): AlgebraOp

}

sealed abstract class SelfPlannedExpr extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet

  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = EvaluateOp(this, wordNet, bindings)
}
/*
 * Imperative expressions
 */
case class EmissionExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = EmitOp(expr.evaluationPlan(wordNet, bindings))
}

case class IteratorExpr(bindingExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings)
    = IterateOp(bindingExpr.evaluationPlan(wordNet, bindings), iteratedExpr.evaluationPlan(wordNet, bindings))
}

case class IfElseExpr(conditionExpr: EvaluableExpr, ifExpr: EvaluableExpr, elseExpr: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = IfElseOp(conditionExpr.evaluationPlan(wordNet, bindings),
    ifExpr.evaluationPlan(wordNet, bindings), elseExpr.map(_.evaluationPlan(wordNet, bindings)))
}

case class BlockExpr(exprs: List[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = BlockOp(exprs.map(_.evaluationPlan(wordNet, bindings)))
}

case class AssignmentExpr(variables: List[Variable], expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = AssignmentOp(variables, expr.evaluationPlan(wordNet, bindings))
}

case class WhileDoExpr(conditionExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings)
    = WhileDoOp(conditionExpr.evaluationPlan(wordNet, bindings), iteratedExpr.evaluationPlan(wordNet, bindings))
}

case class RelationalAliasExpr(name: String, relationalExpr: ArcExprUnion) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    // TODO provide suitable update op
    ConstantOp.empty
  }
}

/*
 * Path expressions
 */
case class BinarySetExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val leftPlan = left.evaluationPlan(wordNet, bindings)
    val rightPlan = right.evaluationPlan(wordNet, bindings)

    op match {
      case "union" =>
        UnionOp(leftPlan, rightPlan)
      case "except" =>
        ExceptOp(leftPlan, rightPlan)
      case "intersect" =>
        IntersectOp(leftPlan, rightPlan)
      case "," =>
        JoinOp(leftPlan, rightPlan)
    }
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmeticExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val leftOp = left.evaluationPlan(wordNet, bindings)
    val rightOp = right.evaluationPlan(wordNet, bindings)

    if (DataType.numeric.contains(leftOp.rightType(0)) && DataType.numeric.contains(rightOp.rightType(0))) {
      op match {
        case "+" =>
          AddOp(leftOp, rightOp)
        case "-" =>
          SubOp(leftOp, rightOp)
        case "*" =>
          MulOp(leftOp, rightOp)
        case "/" =>
          DivOp(leftOp, rightOp)
        case "div" =>
          IntDivOp(leftOp, rightOp)
        case "%" =>
          ModOp(leftOp, rightOp)
      }
    } else {
      throw new WQueryEvaluationException("Operator '" + op +"' requires paths that end with float or integer values")
    }
  }
}

case class MinusExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val op = expr.evaluationPlan(wordNet, bindings)

    if (DataType.numeric.contains(op.rightType(0)))
      MinusOp(op)
    else
      throw new WQueryEvaluationException("Operator '-' requires a path that ends with float or integer values")
  }
}

/* 
 * Function call expressions
 */
case class FunctionExpr(name: String, args: EvaluableExpr) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val avalues = args.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings)
    
    val atypes: List[FunctionArgumentType] = if (avalues.minPathSize != avalues.maxPathSize) {
      List(TupleType)
    } else {
      ((avalues.maxPathSize - 1) to 0 by -1).map(avalues.getType(_)).toList.map { types =>
        if (types.size == 1)
          ValueType(types.head)
        else if (types == Set(FloatType, IntegerType))
          ValueType(FloatType)
        else
          TupleType
      }
    }

    bindings.lookupFunction(name, atypes) 
      .map(invokeFunction(_, atypes, avalues))
      .getOrElse {
        val floatTypes = atypes.map { 
          case ValueType(IntegerType) => ValueType(FloatType)
          case t => t
        }  
          
        bindings.lookupFunction(name, floatTypes)
          .map(invokeFunction(_, floatTypes, avalues))
          .getOrElse {
             bindings.lookupFunction(name, List(TupleType))
               .map(invokeFunction(_, List(TupleType), avalues))
               .getOrElse(throw new WQueryModelException("Function '" + name + "' with argument types " + atypes + " not found"))
          }          
      }
  }
  
  private def invokeFunction(fdesc: (org.wquery.model.Function, java.lang.reflect.Method), atypes: List[FunctionArgumentType], avalues: DataSet) = {
    fdesc match {
      case (func: AggregateFunction, method) =>
        method.invoke(WQueryFunctions, avalues).asInstanceOf[DataSet]
      case (func: ScalarFunction, method) =>
        val buffer = new ListBuffer[List[Any]]()
        val margs = new Array[AnyRef](atypes.size)

        for (tuple <- avalues.paths) {
          for (i <- 0 until margs.size)
            margs(i) = tuple(i).asInstanceOf[AnyRef]

          buffer.append(List(method.invoke(WQueryFunctions, margs: _*)))
        }

        func.result match {
          case ValueType(dtype) =>
            DataSet(buffer.toList)
          case TupleType =>
            throw new RuntimeException("ScalarFunction '" + func.name + "' returned TupleType")
        }
    }      
  }
}

/*
 * Step related expressions
 */
trait VariableBindings {
  def bind(dataSet: DataSet, decls: List[Variable]) = {
    if (decls != Nil) {          
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(decls.filter(x => (x.isInstanceOf[PathVariable] && x.value != "_")).map(_.value))                
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(decls.filterNot(x => (x.isInstanceOf[PathVariable] || x.value == "_")).map(_.value))        
    
      demandUniqueVariableNames(decls)
      
      getPathVariablePosition(decls) match {
        case Some(pathVarPos) => 
          val leftVars = decls.slice(0, pathVarPos).map(_.value).zipWithIndex.filterNot{_._1 == "_"}.toMap
          val rightVars = decls.slice(pathVarPos + 1, decls.size).map(_.value).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
          val pathVarBuffer = if (decls(pathVarPos).value != "_") Some(pathVarBuffers(decls(pathVarPos).value)) else None          
          val pathVarStart = leftVars.size
          val pathVarEnd = rightVars.size
      
          for (tuple <- dataSet.paths) {  
            dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))            
            pathVarBuffer.map(_.append((pathVarStart, tuple.size - pathVarEnd)))
            dataSet.paths.foreach(tuple => bindVariablesFromLeft(leftVars, stepVarBuffers, tuple.size))            
          }
        case None =>
          val rightVars = decls.map(_.value).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap                
          dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))      
      }
    
      DataSet(dataSet.paths, dataSet.pathVars ++ pathVarBuffers.mapValues(_.toList), dataSet.stepVars ++ stepVarBuffers.mapValues(_.toList))
    } else {
      dataSet
    }
  }
  
  private def demandUniqueVariableNames(decls: List[Variable]) {
    val vars = decls.filter(x => !x.isInstanceOf[PathVariable] && x.value != "_").map(_.value)
    
    if (vars.size != vars.distinct.size)
      throw new WQueryEvaluationException("Variable list contains duplicated variable names")
  }
  
  private def getPathVariablePosition(decls: List[Variable]) = {
    val pathVarPos = decls.indexWhere{_.isInstanceOf[PathVariable]}
    
    if (pathVarPos != decls.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryEvaluationException("Variable list '" + decls.map { 
        case PathVariable(v) => "@" + v 
        case StepVariable(v) => "$" + v
      }.mkString + "' contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(pathVarPos)
      else
        None
    }
  }
  
  private def bindVariablesFromLeft(vars: Map[String, Int], varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((v, pos) <- vars)        
      if (pos < tupleSize)
        varIndexes(v).append(pos)
      else 
        throw new WQueryEvaluationException("Variable $" + v + " cannot be bound")
  }    
  
  private def bindVariablesFromRight(vars: Map[String, Int], varIndexes: Map[String, ListBuffer[Int]], tupleSize: Int) {
    for ((v, pos) <- vars) {
      val index = tupleSize - 1 - pos
      
      if (index >= 0)
        varIndexes(v).append(index)
      else 
        throw new WQueryEvaluationException("Variable $" + v + " cannot be bound")
    }
  }
}

sealed abstract class TransformationExpr extends Expr { 
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet): DataSet
}

case class QuantifiedTransformationExpr(chain: PositionedRelationChainTransformationExpr, quantifier: Quantifier) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    quantifier.quantify(ConstantOp(dataSet), chain, wordNet, bindings)
  }
}

case class PositionedRelationTransformationExpr(pos: Int, arcUnion: ArcExprUnion) extends TransformationExpr {
  def demandExtensionPattern(wordNet: WordNet, bindings: Bindings, types: List[Set[DataType]]) = {
    arcUnion.getExtensions(wordNet, types(types.size - pos))
      .map(extensions => ExtensionPattern(pos, extensions))
      .getOrElse(throw new WQueryEvaluationException("Arc expression " + arcUnion + " references an unknown relation or argument"))
  }

  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    val types = dataSet.getType(pos - 1)
    arcUnion.getExtensions(wordNet, if (types.isEmpty) DataType.all else types)
      .map(extensions => ExtendOp(ConstantOp(dataSet), ExtensionPattern(pos, extensions)))
      .getOrElse(throw new WQueryEvaluationException("Arc expression " + arcUnion + " references an unknown relation or argument"))
      .evaluate(wordNet, bindings)
  }
}

case class PositionedRelationChainTransformationExpr(exprs: List[PositionedRelationTransformationExpr]) extends TransformationExpr {
  def demandExtensionPatterns(wordNet: WordNet, bindings: Bindings, types: List[Set[DataType]]) = {
    val patternBuffer = new ListBuffer[ExtensionPattern]
    val typesBuffer = new ListBuffer[Set[DataType]]

    typesBuffer.appendAll(types)

    for (expr <- exprs) {
      val pattern = expr.demandExtensionPattern(wordNet, bindings, typesBuffer.toList)
      val patternExtensionTypes = pattern.extensions.map(extension => extension.to.map(x => extension.relation.arguments(x)))
      val maxSize = patternExtensionTypes.map(_.size).max

      for (i <- 0 until maxSize) {
        val extensionTypes = patternExtensionTypes.filter(_.size <= i).map(x => x(i)).toSet[DataType]
        typesBuffer.append(extensionTypes)
      }

      patternBuffer.append(pattern)
    }

    patternBuffer.toList
  }

  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    exprs.foldLeft(dataSet)((x, expr) => expr.transform(wordNet, bindings, x))
  }
}

case class FilterTransformationExpr(condition: ConditionalExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    SelectOp(ConstantOp(dataSet), condition).evaluate(wordNet, bindings)
  }
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    // bindings.contextVariableType(0) instead of dataSet.getType
    // TODO remove asInstanceOf
    SelectOp(ConstantOp(dataSet), ComparisonExpr("in", AlgebraExpr(ContextRefOp(0, dataSet.getType(0))), generator))
      .evaluate(wordNet, bindings)
  }
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    ProjectOp(ConstantOp(dataSet), expr.evaluationPlan(wordNet, bindings)).evaluate(wordNet, bindings)
  }  	
}

case class BindTransformationExpr(variables: List[Variable]) extends TransformationExpr with VariableBindings {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    BindOp(ConstantOp(dataSet), variables).evaluate(wordNet, bindings)
  }
}

case class PathExpr(generator: EvaluableExpr, steps: List[TransformationExpr]) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    // separate projections
    steps.foldLeft(generator.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings))((step, trans) => trans.transform(wordNet, bindings, step))
  }
}

/*
 * Arc Expressions
 */
case class ArcExpr(ids: List[String]) extends Expr {
  def getExtension(wordNet: WordNet, sourceTypes: Set[DataType]) = {
    (ids: @unchecked) match {
      case List(relationName) =>
        wordNet.getRelation(relationName, sourceTypes, Relation.Source)
          .map(Extension(_, Relation.Source, List(Relation.Destination)))
      case List(left, right) =>
        wordNet.getRelation(left, sourceTypes, Relation.Source)
          .map(Extension(_, Relation.Source, List(right)))
          .orElse(wordNet.getRelation(right, sourceTypes, left).map(Extension(_, left, List(Relation.Destination))))
      case first :: second :: dests =>
        wordNet.getRelation(first, sourceTypes, Relation.Source)
          .map(Extension(_, Relation.Source, second :: dests))
          .orElse((wordNet.getRelation(second, sourceTypes, first).map(Extension(_, first, dests))))
    }
  }

  def getLiteral = if (ids.size == 1) Some(ids.head) else None

  override def toString = ids.mkString("^")
}

case class ArcExprUnion(arcExprs: List[ArcExpr]) extends Expr {
  def getExtensions(wordNet: WordNet, sourceTypes: Set[DataType]) = {
    val patterns = arcExprs.map(_.getExtension(wordNet, sourceTypes))

    if (patterns.filter(_.isDefined).size == arcExprs.size) Some(patterns.map(_.get)) else None
  }

  def getLiteral = if (arcExprs.size == 1) arcExprs.head.getLiteral else None

  override def toString = arcExprs.mkString("|")
}

/*
 * Conditional Expressions
 */
sealed abstract class ConditionalExpr extends Expr {
  def satisfied(wordNet: WordNet, bindings: Bindings): Boolean
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.exists(_.satisfied(wordNet, bindings))
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(wordNet: WordNet, bindings: Bindings) = exprs.forall(_.satisfied(wordNet, bindings))
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def satisfied(wordNet: WordNet, bindings: Bindings) = !expr.satisfied(wordNet, bindings)
}

case class ComparisonExpr(op: String, lexpr: EvaluableExpr, rexpr: EvaluableExpr) extends ConditionalExpr {
  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val lresult = lexpr.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings).paths.map(_.last)
    val rresult = rexpr.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings).paths.map(_.last)

    op match {
      case "=" =>
        lresult.forall(rresult.contains) && rresult.forall(lresult.contains)
      case "!=" =>
        !(lresult.forall(rresult.contains) && rresult.forall(lresult.contains))
      case "in" =>
        lresult.forall(rresult.contains)
      case "pin" =>
        lresult.forall(rresult.contains) && lresult.size < rresult.size
      case "=~" =>
        if (rresult.size == 1 ) {
          // element context
          if (DataType(rresult.head) == StringType) {
            val regex = rresult.head.asInstanceOf[String].r

            lresult.forall {
              case elem: String =>
                regex.findFirstIn(elem).map(_ => true).getOrElse(false)
            }
          } else {
            throw new WQueryEvaluationException("The rightSet side of '" + op +
              "' should return exactly one character string value")
          }
        } else if (rresult.isEmpty) {
          throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns no values")
        } else { // rresult.pathCount > 0
          throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns more than one values")
        }
      case _ =>
        if (lresult.size == 1 && rresult.size == 1) {
          // element context
          op match {
            case "<=" =>
              WQueryFunctions.compare(lresult, rresult) <= 0
            case "<" =>
              WQueryFunctions.compare(lresult, rresult) < 0
            case ">=" =>
              WQueryFunctions.compare(lresult, rresult) >= 0
            case ">" =>
              WQueryFunctions.compare(lresult, rresult) > 0
            case _ =>
              throw new IllegalArgumentException("Unknown comparison operator '" + op + "'")
          }
        } else {
          if (lresult.isEmpty)
            throw new WQueryEvaluationException("The leftSet side of '" + op + "' returns no values")
          if (lresult.size > 1)
            throw new WQueryEvaluationException("The leftSet side of '" + op + "' returns more than one value")
          if (rresult.isEmpty)
            throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns no values")
          if (rresult.size > 1)
            throw new WQueryEvaluationException("The rightSet side of '" + op + "' returns more than one values")

          // the following shall not happen
          throw new WQueryEvaluationException("Both sides of '" + op + "' should return exactly one value")
        }
    }
  }
}

case class PathConditionExpr(expr: PathExpr) extends ConditionalExpr {
  def satisfied(wordNet: WordNet, bindings: Bindings) = {
    val lastSteps = expr.evaluate(wordNet, bindings).paths.map(_.last)

    !lastSteps.isEmpty && lastSteps.forall(x => x.isInstanceOf[Boolean] && x.asInstanceOf[Boolean])
  }
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val op = expr.evaluationPlan(wordNet, bindings)
    val contextTypes = op.rightType(0).filter(t => t == StringType || t == SenseType)

    if (!contextTypes.isEmpty) {
      val extensions = contextTypes.map{ contextType => (contextType: @unchecked) match {
        case StringType =>
          Extension(WordNet.WordFormToSynsets, Relation.Source, List(Relation.Destination))
        case SenseType =>
          Extension(WordNet.SenseToSynset, Relation.Source, List(Relation.Destination))
      }}.toList

      ProjectOp(ExtendOp(op, ExtensionPattern(1, extensions)), ContextRefOp(0, contextTypes))
    } else {
      throw new WQueryEvaluationException("{...} requires an expression that generates either senses or word forms")
    }
  }
}

case class ContextByArcExprUnionReq(arcUnion: ArcExprUnion, quantifier: Quantifier) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val chain = PositionedRelationChainTransformationExpr(List(PositionedRelationTransformationExpr(1, arcUnion)))
    val arcOp = if (bindings.areContextVariablesBound) {
      arcUnion.getExtensions(wordNet, Set(bindings.contextVariableType(0))).map(_ => ContextRefOp(0, Set(bindings.contextVariableType(0))))
        .map(op => ConstantOp(quantifier.quantify(op, chain, wordNet, bindings)))
    } else {
      arcUnion.getExtensions(wordNet, DataType.all).map(_.zip(arcUnion.arcExprs).map {
        case (pattern, expr) =>
          if (expr.getLiteral.isDefined)
            FetchOp.relationTuplesByArgumentNames(pattern.relation, pattern.relation.argumentNames)
          else
            FetchOp.relationTuplesByArgumentNames(pattern.relation, pattern.from::pattern.to)
      }).map(extensions => extensions.tail.foldLeft(UnionOp(extensions.head, ConstantOp.empty))((sum, elem) => UnionOp(sum, elem))) // TODO optimize
        .map(op => ConstantOp(quantifier.decrement.quantify(op, chain, wordNet, bindings)))
    }

    arcOp.getOrElse(arcUnion.getLiteral.map(FetchOp.wordByValue(_))
      .getOrElse(throw new WQueryEvaluationException("Expression " + arcUnion + " contains an invalid relation or argument name")))
  }
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    // TODO bindings.contextVariableType(0)
    SelectOp(FetchOp.words, ComparisonExpr("=~", AlgebraExpr(ContextRefOp(0, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))))
  }
}

case class ContextReferenceReq(pos: Int) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = ContextRefOp(pos, Set(bindings.contextVariableType(pos)))
}

case class BooleanByFilterReq(condition: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = IfElseOp(SelectOp(ConstantOp.fromValue(true), condition),
    ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = variable match {
    case PathVariable(name) =>
      if (bindings.isPathVariableBound(name))
        PathVariableRefOp(name, bindings.pathVariableType(name))
      else
        throw new WQueryEvaluationException("A reference to unknown variable @'" + name + "' found")
    case StepVariable(name) =>
      if (bindings.isStepVariableBound(name))
        StepVariableRefOp(name, bindings.stepVariableType(name))
      else
        throw new WQueryEvaluationException("A reference to unknown variable $'" + name + "' found")
  }
}

case class ArcByArcExprReq(arcExpr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    arcExpr.getExtension(wordNet, DataType.all)
      .map(pattern => ConstantOp(DataSet(pattern.to.map(to => List(Arc(pattern.relation, pattern.from, to))))))
      .getOrElse(throw new WQueryEvaluationException("Arc generator " + arcExpr + " references an unknown relation or argument"))
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = op
}

/*
 * Variables
 */
sealed abstract class Variable(val value: String) extends Expr

case class StepVariable(override val value: String) extends Variable(value)
case class PathVariable(override val value: String) extends Variable(value)

/*
 * Quantifier
 */
case class Quantifier(val lowerBound: Int, val upperBound: Option[Int]) extends Expr {
  def quantify(op: AlgebraOp, chain: PositionedRelationChainTransformationExpr, wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)
    val lowerResult = (1 to lowerBound).foldLeft(dataSet)((x, _) => chain.transform(wordNet, bindings, x))

    if (!lowerResult.isEmpty && Some(lowerBound) != upperBound)
      CloseOp(ConstantOp(lowerResult), chain.demandExtensionPatterns(wordNet, bindings, lowerResult.types), upperBound.map(_ - lowerBound)).evaluate(wordNet, bindings)
    else
      lowerResult
  }

  def decrement = Quantifier(lowerBound - 1, upperBound.map(_ - 1))
}