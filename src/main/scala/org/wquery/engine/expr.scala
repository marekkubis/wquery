package org.wquery.engine
import org.wquery.{WQueryEvaluationException, WQueryModelException}
import scala.collection.mutable.ListBuffer
import org.wquery.model._
import java.lang.reflect.Method

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
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    BlockOp(exprs.map(_.evaluationPlan(wordNet, bindings)))
  }
}

case class AssignmentExpr(variables: List[Variable], expr: EvaluableExpr) extends EvaluableExpr with VariableTypeBindings {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val op = expr.evaluationPlan(wordNet, bindings)

    bind(bindings, op, variables)
    AssignmentOp(variables, op)
  }
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
case class FunctionExpr(name: String, args: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    val argsOp = args.evaluationPlan(wordNet, bindings)

    val argsTypes: List[FunctionArgumentType] = if (argsOp.maxTupleSize.map(_ != argsOp.minTupleSize).getOrElse(true)) {
      List(TupleType)
    } else {
      ((argsOp.minTupleSize - 1) to 0 by -1).map(argsOp.rightType(_)).toList.map { types =>
        if (types.size == 1)
          ValueType(types.head)
        else if (types == Set(FloatType, IntegerType))
          ValueType(FloatType)
        else
          TupleType
      }
    }

    bindings.lookupFunction(name, argsTypes)
      .map(invokeFunction(_, argsOp))
      .getOrElse {
        val floatTypes = argsTypes.map {
          case ValueType(IntegerType) => ValueType(FloatType)
          case t => t
        }

        bindings.lookupFunction(name, floatTypes)
          .map(invokeFunction(_, argsOp))
          .getOrElse {
             bindings.lookupFunction(name, List(TupleType))
               .map(invokeFunction(_, argsOp))
               .getOrElse(throw new WQueryModelException("Function '" + name + "' with argument types " + argsTypes + " not found"))
          }
      }
  }

  private def invokeFunction(functionDescription: (Function, Method), argsOp: AlgebraOp) = {
    functionDescription._1 match {
      case _: AggregateFunction =>
        AggregateFunctionOp(functionDescription._1, functionDescription._2, argsOp)
      case _: ScalarFunction =>
        ScalarFunctionOp(functionDescription._1, functionDescription._2, argsOp)
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

trait VariableTypeBindings {
  def bind(bindings: Bindings, op: AlgebraOp, variables: List[Variable]) {
    demandUniqueVariableNames(variables)

    getPathVariableNameAndPos(variables) match {
      case Some((pathVarName, pathVarPos)) =>
        val leftVars = variables.slice(0, pathVarPos).map(_.value).zipWithIndex.filterNot{_._1 == "_"}.toMap
        val rightVars = variables.slice(pathVarPos + 1, variables.size).map(_.value).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap

        bindVariablesFromRight(bindings, op, rightVars)

        if (pathVarName != "_")
          bindings.bindPathVariableType(pathVarName, op, pathVarPos, variables.size - pathVarPos - 1)

        bindVariablesFromLeft(bindings, op, leftVars)
      case None =>
        val rightVars = variables.map(_.value).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
        bindVariablesFromRight(bindings, op, rightVars)
    }
  }

  private def demandUniqueVariableNames(variables: List[Variable]) {
    val vars = variables.filter(x => !x.isInstanceOf[PathVariable] && x.value != "_").map(_.value)

    if (vars.size != vars.distinct.size)
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains duplicated variable names")
  }

  private def getPathVariableNameAndPos(variables: List[Variable]) = {
    val pathVarPos = variables.indexWhere{_.isInstanceOf[PathVariable]}

    if (pathVarPos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(variables(pathVarPos).value, pathVarPos)
      else
        None
    }
  }

  private def bindVariablesFromLeft(bindings: Bindings, op: AlgebraOp, vars: Map[String, Int]) {
    // TODO bind variables from left
  }

  private def bindVariablesFromRight(bindings: Bindings, op: AlgebraOp, vars: Map[String, Int]) {
    for ((name, pos) <- vars) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindings.bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryEvaluationException("Variable $" + name + " cannot be bound")
    }
  }
}

sealed abstract class TransformationExpr extends Expr { 
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet): DataSet

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp): AlgebraOp
}

case class QuantifiedTransformationExpr(chain: PositionedRelationChainTransformationExpr, quantifier: Quantifier) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    quantifier.quantify(ConstantOp(dataSet), chain, wordNet, bindings).evaluate(wordNet, bindings)
  }

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    quantifier.quantify(op, chain, wordNet, bindings)
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

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    val types = op.rightType(pos - 1)
    arcUnion.getExtensions(wordNet, if (types.isEmpty) DataType.all else types)
      .map(extensions => ExtendOp(op, ExtensionPattern(pos, extensions)))
      .getOrElse(throw new WQueryEvaluationException("Arc expression " + arcUnion + " references an unknown relation or argument"))
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

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    exprs.foldLeft(op)((x, expr) => expr.transformPlan(wordNet, bindings, x))
  }
}

case class FilterTransformationExpr(condition: ConditionalExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    SelectOp(ConstantOp(dataSet), condition).evaluate(wordNet, bindings)
  }

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = SelectOp(op, condition)
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    // TODO replace bindings.contextVariableType(0) instead of dataSet.getType
    SelectOp(ConstantOp(dataSet), ComparisonExpr("in", AlgebraExpr(ContextRefOp(0, dataSet.getType(0))), generator))
      .evaluate(wordNet, bindings)
  }

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    SelectOp(op, ComparisonExpr("in", AlgebraExpr(ContextRefOp(0, op.rightType(0))), generator))
  }
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    ProjectOp(ConstantOp(dataSet), expr.evaluationPlan(wordNet, bindings)).evaluate(wordNet, bindings)
  }

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    ProjectOp(op, expr.evaluationPlan(wordNet, bindings))
  }
}

case class BindTransformationExpr(variables: List[Variable]) extends TransformationExpr with VariableTypeBindings {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    // TODO remove the eager evaluation below
    BindOp(ConstantOp(dataSet), variables).evaluate(wordNet, bindings)
  }

  def transformPlan(wordNet: WordNet, bindings: Bindings, op: AlgebraOp) = {
    bind(bindings, op, variables)
    BindOp(op, variables)
  }
}

case class PathExpr(generator: EvaluableExpr, steps: List[TransformationExpr]) extends EvaluableExpr {
//  def evaluate(wordNet: WordNet, bindings: Bindings) = {
//    // separate projections
//    steps.foldLeft(generator.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings))((step, trans) => trans.transform(wordNet, bindings, step))
//  }

  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = {
    steps.foldLeft(generator.evaluationPlan(wordNet, bindings))((step, trans) => trans.transformPlan(wordNet, bindings, step))
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
    val lastSteps = expr.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings).paths.map(_.last)

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

      ProjectOp(ExtendOp(op, ExtensionPattern(1, extensions)), ContextRefOp(0, Set(SynsetType)))
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
        .map(op => quantifier.quantify(op, chain, wordNet, bindings))
    } else {
      arcUnion.getExtensions(wordNet, DataType.all).map(_.zip(arcUnion.arcExprs).map {
        case (pattern, expr) =>
          if (expr.getLiteral.isDefined)
            FetchOp.relationTuplesByArgumentNames(pattern.relation, pattern.relation.argumentNames)
          else
            FetchOp.relationTuplesByArgumentNames(pattern.relation, pattern.from::pattern.to)
      }).map(extensions => extensions.tail.foldLeft(UnionOp(extensions.head, ConstantOp.empty))((sum, elem) => UnionOp(sum, elem))) // TODO optimize
        .map(op => quantifier.decrement.quantify(op, chain, wordNet, bindings))
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
      bindings.lookupPathVariableType(name).map(PathVariableRefOp(name, _))
        .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable @" + name + " found"))
    case StepVariable(name) =>
      bindings.lookupStepVariableType(name).map(StepVariableRefOp(name, _))
        .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable $" + name + " found"))
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

case class StepVariable(override val value: String) extends Variable(value) {
  override def toString = "$" + value
}

case class PathVariable(override val value: String) extends Variable(value) {
  override def toString = "@" + value
}

/*
 * Quantifier
 */
case class Quantifier(val lowerBound: Int, val upperBound: Option[Int]) extends Expr {
  def quantify(op: AlgebraOp, chain: PositionedRelationChainTransformationExpr, wordNet: WordNet, bindings: Bindings) = {
    val lowerOp = (1 to lowerBound).foldLeft(op)((x, _) => chain.transformPlan(wordNet, bindings, x))
    val furthestReference = chain.exprs.map(_.pos).max - 1
    val typeReferences = (for (i <- furthestReference to 0 by -1) yield lowerOp.rightType(i)).toList

    if (Some(lowerBound) != upperBound)
      CloseOp(lowerOp, chain.demandExtensionPatterns(wordNet, bindings, typeReferences), upperBound.map(_ - lowerBound))
    else
      lowerOp
  }

  def decrement = Quantifier(lowerBound - 1, upperBound.map(_ - 1))
}