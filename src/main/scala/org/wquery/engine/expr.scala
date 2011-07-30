package org.wquery.engine
import org.wquery.{WQueryEvaluationException, WQueryModelException}
import scala.collection.mutable.ListBuffer
import org.wquery.model._
import java.lang.reflect.Method

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema): AlgebraOp
}

/*
 * Imperative expressions
 */
case class EmissionExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = EmitOp(expr.evaluationPlan(wordNet, bindings))
}

case class IteratorExpr(bindingExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val bindingsOp = bindingExpr.evaluationPlan(wordNet, bindings)
    IterateOp(bindingsOp, iteratedExpr.evaluationPlan(wordNet, bindings union bindingsOp.bindingsSchema))
  }
}

case class IfElseExpr(conditionExpr: EvaluableExpr, ifExpr: EvaluableExpr, elseExpr: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = IfElseOp(conditionExpr.evaluationPlan(wordNet, bindings),
    ifExpr.evaluationPlan(wordNet, bindings), elseExpr.map(_.evaluationPlan(wordNet, bindings)))
}

case class BlockExpr(exprs: List[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val blockBindings = BindingsSchema(bindings, true)

    BlockOp(exprs.map(expr => expr.evaluationPlan(wordNet, blockBindings)))
  }
}

case class AssignmentExpr(variables: List[Variable], expr: EvaluableExpr) extends EvaluableExpr with VariableTypeBindings {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val op = expr.evaluationPlan(wordNet, bindings)

    bindTypes(bindings, op, variables)
    AssignmentOp(variables, op)
  }
}

case class WhileDoExpr(conditionExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema)
    = WhileDoOp(conditionExpr.evaluationPlan(wordNet, bindings), iteratedExpr.evaluationPlan(wordNet, bindings))
}

case class RelationalAliasExpr(name: String, relationalExpr: ArcExprUnion) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    // TODO provide suitable update op
    ConstantOp.empty
  }
}

/*
 * Path expressions
 */
case class BinarySetExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
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

case class ConjunctiveExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    expr.evaluationPlan(wordNet, BindingsSchema(bindings, false))
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmeticExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
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
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
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
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val argsOp = args.evaluationPlan(wordNet, bindings)

    Functions.findFunctionsByName(name).map { functions =>
      functions.find(_.accepts(argsOp)).map(FunctionOp(_, argsOp))
        .getOrElse(throw new WQueryEvaluationException("Function '" + name + "' cannot accept provided arguments"))
    }.getOrElse(throw new WQueryEvaluationException("Function '" + name + "' not found"))
  }
}

/*
 * Step related expressions
 */
trait VariableBindings {
  def bind(dataSet: DataSet, variables: List[Variable]) = {
    if (variables != Nil) {
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(variables.filter(x => (x.isInstanceOf[PathVariable] && x.name != "_")).map(_.name))
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(variables.filterNot(x => (x.isInstanceOf[PathVariable] || x.name == "_")).map(_.name))
    
      demandUniqueVariableNames(variables)
      
      getPathVariablePosition(variables) match {
        case Some(pathVarPos) => 
          val leftVars = variables.slice(0, pathVarPos).map(_.name).zipWithIndex.filterNot{_._1 == "_"}.toMap
          val rightVars = variables.slice(pathVarPos + 1, variables.size).map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
          val pathVarBuffer = if (variables(pathVarPos).name != "_") Some(pathVarBuffers(variables(pathVarPos).name)) else None
          val pathVarStart = leftVars.size
          val pathVarEnd = rightVars.size
      
          for (tuple <- dataSet.paths) {  
            dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))            
            pathVarBuffer.map(_.append((pathVarStart, tuple.size - pathVarEnd)))
            dataSet.paths.foreach(tuple => bindVariablesFromLeft(leftVars, stepVarBuffers, tuple.size))            
          }
        case None =>
          val rightVars = variables.map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
          dataSet.paths.foreach(tuple => bindVariablesFromRight(rightVars, stepVarBuffers, tuple.size))      
      }
    
      DataSet(dataSet.paths, dataSet.pathVars ++ pathVarBuffers.mapValues(_.toList), dataSet.stepVars ++ stepVarBuffers.mapValues(_.toList))
    } else {
      dataSet
    }
  }
  
  private def demandUniqueVariableNames(variables: List[Variable]) {
    val variableNames = variables.filter(x => !x.isInstanceOf[PathVariable] && x.name != "_").map(_.name)
    
    if (variableNames.size != variableNames.distinct.size)
      throw new WQueryEvaluationException("Variable list contains duplicated variable names")
  }
  
  private def getPathVariablePosition(variables: List[Variable]) = {
    val pathVarPos = variables.indexWhere{_.isInstanceOf[PathVariable]}
    
    if (pathVarPos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryEvaluationException("Variable list '" + variables.map {
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
  def bindTypes(bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) {
    demandUniqueVariableNames(variables)

    getPathVariableNameAndPos(variables) match {
      case Some((pathVarName, pathVarPos)) =>
        val leftVars = variables.slice(0, pathVarPos).map(_.name).zipWithIndex.filterNot{_._1 == "_"}.toMap
        val rightVars = variables.slice(pathVarPos + 1, variables.size).map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap

        bindVariablesFromRight(bindings, op, rightVars)

        if (pathVarName != "_")
          bindings.bindPathVariableType(pathVarName, op, pathVarPos, variables.size - pathVarPos - 1)

        bindVariablesFromLeft(bindings, op, leftVars)
      case None =>
        val rightVars = variables.map(_.name).reverse.zipWithIndex.filterNot{_._1 == "_"}.toMap
        bindVariablesFromRight(bindings, op, rightVars)
    }
  }

  private def demandUniqueVariableNames(variables: List[Variable]) {
    val vars = variables.filter(x => !x.isInstanceOf[PathVariable] && x.name != "_").map(_.name)

    if (vars.size != vars.distinct.size)
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains duplicated variable names")
  }

  private def getPathVariableNameAndPos(variables: List[Variable]) = {
    val pathVarPos = variables.indexWhere{_.isInstanceOf[PathVariable]}

    if (pathVarPos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryEvaluationException("Variable list " + variables.mkString + " contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(variables(pathVarPos).name, pathVarPos)
      else
        None
    }
  }

  private def bindVariablesFromLeft(bindings: BindingsSchema, op: AlgebraOp, vars: Map[String, Int]) {
    for ((name, pos) <- vars) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindings.bindStepVariableType(name, op.leftType(pos))
      else
        throw new WQueryEvaluationException("Variable $" + name + " cannot be bound")
    }
  }

  private def bindVariablesFromRight(bindings: BindingsSchema, op: AlgebraOp, vars: Map[String, Int]) {
    for ((name, pos) <- vars) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindings.bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryEvaluationException("Variable $" + name + " cannot be bound")
    }
  }
}

sealed abstract class TransformationExpr extends Expr {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp): AlgebraOp
}

case class QuantifiedTransformationExpr(chain: PositionedRelationChainTransformationExpr, quantifier: Quantifier) extends TransformationExpr {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    quantifier.quantify(op, chain, wordNet, bindings)
  }
}

case class PositionedRelationTransformationExpr(pos: Int, arcUnion: ArcExprUnion) extends TransformationExpr {
  def demandExtensionPattern(wordNet: WordNet, bindings: BindingsSchema, types: List[Set[DataType]]) = {
    arcUnion.getExtensions(wordNet, types(types.size - pos))
      .map(extensions => ExtensionPattern(pos, extensions))
      .getOrElse(throw new WQueryEvaluationException("Arc expression " + arcUnion + " references an unknown relation or argument"))
  }

  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    val types = op.rightType(pos - 1)
    arcUnion.getExtensions(wordNet, if (types.isEmpty) DataType.all else types)
      .map(extensions => ExtendOp(op, ExtensionPattern(pos, extensions)))
      .getOrElse(throw new WQueryEvaluationException("Arc expression " + arcUnion + " references an unknown relation or argument"))
  }
}

case class PositionedRelationChainTransformationExpr(exprs: List[PositionedRelationTransformationExpr]) extends TransformationExpr {
  def demandExtensionPatterns(wordNet: WordNet, bindings: BindingsSchema, types: List[Set[DataType]]) = {
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

  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    exprs.foldLeft(op)((x, expr) => expr.transformPlan(wordNet, bindings, x))
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    SelectOp(op, conditionalExpr.conditionPlan(wordNet, filterBindings))
  }
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    SelectOp(op, BinaryConditionalExpr("in", AlgebraExpr(ContextRefOp(0, op.rightType(0))), generator).conditionPlan(wordNet, filterBindings))
  }
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    ProjectOp(op, expr.evaluationPlan(wordNet, bindings))
  }
}

case class BindTransformationExpr(variables: List[Variable]) extends TransformationExpr with VariableTypeBindings {
  def transformPlan(wordNet: WordNet, bindings: BindingsSchema, op: AlgebraOp) = {
    bindTypes(bindings, op, variables)
    BindOp(op, variables)
  }
}

case class PathExpr(generator: EvaluableExpr, steps: List[TransformationExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
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
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema): Condition
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema) = OrCondition(exprs.map(_.conditionPlan(wordNet, bindings)))
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema) = AndCondition(exprs.map(_.conditionPlan(wordNet, bindings)))
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema) = NotCondition(expr.conditionPlan(wordNet, bindings))
}

case class BinaryConditionalExpr(op: String, leftExpr: EvaluableExpr, rightExpr: EvaluableExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val leftOp = leftExpr.evaluationPlan(wordNet, bindings)
    val rightOp = rightExpr.evaluationPlan(wordNet, bindings)

    op match {
      case "=" =>
        BinaryCondition(op, leftOp, rightOp)

      case "!=" =>
        BinaryCondition(op, leftOp, rightOp)
      case "in" =>
        BinaryCondition(op, leftOp, rightOp)
      case "pin" =>
        BinaryCondition(op, leftOp, rightOp)
      case "=~" =>
        BinaryCondition(op, leftOp, rightOp)
      case "<=" =>
        BinaryCondition(op, leftOp, rightOp)
      case "<" =>
        BinaryCondition(op, leftOp, rightOp)
      case ">=" =>
        BinaryCondition(op, leftOp, rightOp)
      case ">" =>
        BinaryCondition(op, leftOp, rightOp)
      case _ =>
        throw new IllegalArgumentException("Unknown comparison operator '" + op + "'")
    }
  }
}

case class PathConditionExpr(expr: PathExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet, bindings: BindingsSchema) = RightFringeCondition(expr.evaluationPlan(wordNet, bindings))
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
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
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    val chain = PositionedRelationChainTransformationExpr(List(PositionedRelationTransformationExpr(1, arcUnion)))
    val arcOp = if (bindings.areContextVariablesBound) {
      arcUnion.getExtensions(wordNet, bindings.lookupContextVariableType(0)).map(_ => ContextRefOp(0, bindings.lookupContextVariableType(0)))
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
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    SelectOp(FetchOp.words, BinaryConditionalExpr("=~", AlgebraExpr(ContextRefOp(0, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))).conditionPlan(wordNet, bindings))
  }
}

case class ContextReferenceReq(pos: Int) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = ContextRefOp(pos, bindings.lookupContextVariableType(pos))
}

case class BooleanByFilterReq(conditionalExpr: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    IfElseOp(SelectOp(ConstantOp.fromValue(true), conditionalExpr.conditionPlan(wordNet, bindings)),
      ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = variable match {
    case PathVariable(name) =>
      bindings.lookupPathVariableType(name).map(PathVariableRefOp(name, _))
        .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable @" + name + " found"))
    case StepVariable(name) =>
      bindings.lookupStepVariableType(name).map(StepVariableRefOp(name, _))
        .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable $" + name + " found"))
  }
}

case class ArcByArcExprReq(arcExpr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = {
    arcExpr.getExtension(wordNet, DataType.all)
      .map(pattern => ConstantOp(DataSet(pattern.to.map(to => List(Arc(pattern.relation, pattern.from, to))))))
      .getOrElse(throw new WQueryEvaluationException("Arc generator " + arcExpr + " references an unknown relation or argument"))
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: BindingsSchema) = op
}

/*
 * Variables
 */
sealed abstract class Variable(val name: String) extends Expr

case class StepVariable(override val name: String) extends Variable(name) {
  override def toString = "$" + name
}

case class PathVariable(override val name: String) extends Variable(name) {
  override def toString = "@" + name
}

/*
 * Quantifier
 */
case class Quantifier(lowerBound: Int, upperBound: Option[Int]) extends Expr {
  def quantify(op: AlgebraOp, chain: PositionedRelationChainTransformationExpr, wordNet: WordNet, bindings: BindingsSchema) = {
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