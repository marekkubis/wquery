package org.wquery.engine
import scala.collection.mutable.ListBuffer
import org.wquery.model._
import org.wquery.{WQueryStaticCheckException, WQueryEvaluationException}

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema): AlgebraOp
}

/*
 * Imperative expressions
 */
case class EmissionExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = EmitOp(expr.evaluationPlan(wordNet, bindings))
}

case class IteratorExpr(bindingExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val bindingsOp = bindingExpr.evaluationPlan(wordNet, bindings)
    IterateOp(bindingsOp, iteratedExpr.evaluationPlan(wordNet, bindings union bindingsOp.bindingsPattern))
  }
}

case class IfElseExpr(conditionExpr: EvaluableExpr, ifExpr: EvaluableExpr, elseExpr: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = IfElseOp(conditionExpr.evaluationPlan(wordNet, bindings),
    ifExpr.evaluationPlan(wordNet, bindings), elseExpr.map(_.evaluationPlan(wordNet, bindings)))
}

case class BlockExpr(exprs: List[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val blockBindings = BindingsSchema(bindings, true)

    BlockOp(exprs.map(expr => expr.evaluationPlan(wordNet, blockBindings)))
  }
}

case class WhileDoExpr(conditionExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema)
    = WhileDoOp(conditionExpr.evaluationPlan(wordNet, bindings), iteratedExpr.evaluationPlan(wordNet, bindings))
}

case class VariableAssignmentExpr(variables: List[Variable], expr: EvaluableExpr) extends EvaluableExpr with VariableTypeBindings {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val op = expr.evaluationPlan(wordNet, bindings)

    bindTypes(bindings, op, variables)
    AssignmentOp(variables, op)
  }
}

case class RelationAssignmentExpr(name: String, expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val pattern = expr.evaluationPattern(wordNet, DataType.all)
    val sourceType = demandSingleNodeType(pattern.sourceType, "source")
    val destinationType = demandSingleNodeType(pattern.rightType(0), "destination")

    CreateRelationFromPatternOp(name, pattern, sourceType, destinationType)
  }

  private def demandSingleNodeType(types: Set[_ <: DataType], typeName: String) = {
    if (types.size == 1 && types.head.isInstanceOf[NodeType])
      types.head.asInstanceOf[NodeType]
    else
      throw new WQueryEvaluationException("Expression " + expr + " does not determine single " + typeName + " type")
  }
}

/*
 * Path expressions
 */
case class BinarySetExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
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
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    expr.evaluationPlan(wordNet, BindingsSchema(bindings, false))
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmeticExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val leftOp = left.evaluationPlan(wordNet, bindings)
    val rightOp = right.evaluationPlan(wordNet, bindings)

    if (leftOp.rightType(0).subsetOf(DataType.numeric) && rightOp.rightType(0).subsetOf(DataType.numeric)) {
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
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val op = expr.evaluationPlan(wordNet, bindings)

    if (op.rightType(0).subsetOf(DataType.numeric))
      MinusOp(op)
    else
      throw new WQueryEvaluationException("Operator '-' requires a path that ends with float or integer values")
  }
}

/* 
 * Function call expressions
 */
case class FunctionExpr(name: String, args: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
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
trait VariableTypeBindings {
  def bindTypes(bindings: BindingsPattern, op: AlgebraOp, variables: List[Variable]) {
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
      throw new WQueryStaticCheckException("Variable list " + variables.mkString + " contains duplicated variable names")
  }

  private def getPathVariableNameAndPos(variables: List[Variable]) = {
    val pathVarPos = variables.indexWhere{_.isInstanceOf[PathVariable]}

    if (pathVarPos != variables.lastIndexWhere{_.isInstanceOf[PathVariable]}) {
      throw new WQueryStaticCheckException("Variable list " + variables.mkString + " contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(variables(pathVarPos).name, pathVarPos)
      else
        None
    }
  }

  private def bindVariablesFromLeft(bindings: BindingsPattern, op: AlgebraOp, vars: Map[String, Int]) {
    for ((name, pos) <- vars) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindings.bindStepVariableType(name, op.leftType(pos))
      else
        throw new WQueryStaticCheckException("Variable $" + name + " cannot be bound")
    }
  }

  private def bindVariablesFromRight(bindings: BindingsPattern, op: AlgebraOp, vars: Map[String, Int]) {
    for ((name, pos) <- vars) {
      if (op.maxTupleSize.map(pos < _).getOrElse(true))
        bindings.bindStepVariableType(name, op.rightType(pos))
      else
        throw new WQueryStaticCheckException("Variable $" + name + " cannot be bound")
    }
  }
}

case class StepExpr(expr: TransformationExpr, variables: List[Variable]) extends Expr with VariableTypeBindings {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp) = {
    expr.step(wordNet, bindings, op, variables)
  }
}

sealed abstract class TransformationExpr extends Expr with VariableTypeBindings {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]): (AlgebraOp, Step)

  def bind(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) = {
    if (variables.isEmpty) {
      op
    } else {
      bindTypes(bindings, op, variables)
      BindOp(op, variables)
    }
  }
}

case class RelationTransformationExpr(pos: Int, expr: RelationalExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) = {
    val pattern = expr.evaluationPattern(wordNet, op.rightType(pos))
    (bind(wordNet, bindings, ExtendOp(op, pos, pattern), variables), RelationStep(pos, pattern, variables))
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    val condition = conditionalExpr.conditionPlan(wordNet, filterBindings)
    (bind(wordNet, bindings, SelectOp(op, condition), variables), FilterStep(condition, variables))
  }
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) = {
    if (op != ConstantOp.empty) {
      val filterBindings = BindingsSchema(bindings, false)
      filterBindings.bindContextOp(op)
      val generateOp = generator.evaluationPlan(wordNet, filterBindings)

      (bind(wordNet, bindings, SelectOp(op, BinaryCondition("in", ContextRefOp(0, op.rightType(0)), generateOp)), variables), NodeStep(generateOp, variables))
    } else {
      val generateOp = bind(wordNet, bindings, generator.evaluationPlan(wordNet, bindings), variables)

      (generateOp, NodeStep(generateOp, variables))
    }
  }
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, op: AlgebraOp, variables: List[Variable]) = {
    val projectOp = expr.evaluationPlan(wordNet, bindings)
    (bind(wordNet, bindings, ProjectOp(op, projectOp), variables), ProjectStep(projectOp, variables))
  }
}

sealed abstract class RelationalExpr extends Expr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]): RelationalPattern
}

case class RelationUnionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    if (exprs.size == 1)
      exprs.head.evaluationPattern(wordNet, sourceTypes)
    else
      RelationUnionPattern(exprs.map(_.evaluationPattern(wordNet, sourceTypes)))
  }
}

case class RelationCompositionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    val headPattern = exprs.head.evaluationPattern(wordNet, sourceTypes)

    if (exprs.size == 1) {
      headPattern
    } else {
      val buffer = new ListBuffer[RelationalPattern]

      buffer.append(headPattern)
      exprs.tail.foreach(expr => buffer.append(expr.evaluationPattern(wordNet, buffer.last.rightType(0))))
      RelationCompositionPattern(buffer.toList)
    }
  }
}

case class QuantifiedRelationExpr(expr: RelationalExpr, quantifier: Quantifier) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    QuantifiedRelationPattern(expr.evaluationPattern(wordNet, sourceTypes), quantifier)
  }
}

case class ArcExpr(ids: List[ArcExprArgument]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, contextTypes: Set[DataType]) = {
    ((ids: @unchecked) match {
      case List(arg) =>
        if (arg.nodeType.isEmpty) {
          wordNet.getRelation(arg.name, contextTypes, Relation.Source)
            .map(ArcPattern(_, Relation.Source, List(Relation.Destination)))
        } else {
          throw new WQueryStaticCheckException("Relation name " + arg.name + " cannot be followed by type specifier &")
        }
      case left::right::rest =>
        if (left.nodeType.isDefined && right.nodeType.isDefined) {
          throw new WQueryStaticCheckException("No relation name found in arc expression " + toString)
        } else if (left.nodeType.isDefined) {
          evaluateAsSourceTypePattern(wordNet, contextTypes, left, right, rest)
        } else if (right.nodeType.isDefined) {
          evaluateAsDestinationTypePattern(wordNet, contextTypes, left, right, rest)
        } else {
          evaluateAsDestinationTypePattern(wordNet, contextTypes, left, right, rest)
            .orElse(evaluateAsSourceTypePattern(wordNet, contextTypes, left, right, rest))
        }
    }).getOrElse(throw new WQueryStaticCheckException("Arc expression " + toString + " references an unknown relation or argument"))
  }

  private def evaluateAsSourceTypePattern(wordNet: WordNetSchema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument, rest: List[ArcExprArgument]) = {
    val sourceTypes = left.nodeType.map(Set[DataType](_)).getOrElse(contextTypes)

    wordNet.getRelation(right.name, sourceTypes, left.name)
      .map(ArcPattern(_, left.name, if (rest.isEmpty) List(Relation.Destination) else rest.map(_.name)))
  }

  private def evaluateAsDestinationTypePattern(wordNet: WordNetSchema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument, rest: List[ArcExprArgument]) = {
    wordNet.getRelation(left.name, contextTypes, Relation.Source)
      .map(ArcPattern(_, Relation.Source, (right::rest).map(_.name)))
  }

  override def toString = ids.mkString("^")
}

case class ArcExprArgument(name: String, nodeTypeName: Option[String]) extends Expr {
  def nodeType: Option[NodeType] = nodeTypeName.map(n => NodeType.fromName(n))

  override def toString = name + nodeTypeName.getOrElse("")
}

case class PathExpr(exprs: List[StepExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val buffer = new ListBuffer[Step]

    exprs.foldLeft[AlgebraOp](ConstantOp.empty) { (contextOp, expr) =>
      val stepTuple = expr.step(wordNet, bindings, contextOp)
      buffer.append(stepTuple._2)
      stepTuple._1
    }

    PathExprPlanner.plan(buffer.toList, wordNet, bindings)
  }
}

/*
 * Conditional Expressions
 */
sealed abstract class ConditionalExpr extends Expr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema): Condition
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = OrCondition(exprs.map(_.conditionPlan(wordNet, bindings)))
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = AndCondition(exprs.map(_.conditionPlan(wordNet, bindings)))
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = NotCondition(expr.conditionPlan(wordNet, bindings))
}

case class BinaryConditionalExpr(op: String, leftExpr: EvaluableExpr, rightExpr: EvaluableExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
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
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = RightFringeCondition(expr.evaluationPlan(wordNet, bindings))
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val op = expr.evaluationPlan(wordNet, bindings)
    val contextTypes = op.rightType(0).filter(t => t == StringType || t == SenseType)

    if (!contextTypes.isEmpty) {
      val patterns = contextTypes.map{ contextType => (contextType: @unchecked) match {
        case StringType =>
          ArcPattern(WordNet.WordFormToSynsets, Relation.Source, List(Relation.Destination))
        case SenseType =>
          ArcPattern(WordNet.SenseToSynset, Relation.Source, List(Relation.Destination))
      }}.toList

      ProjectOp(ExtendOp(op, 0, RelationUnionPattern(patterns)), ContextRefOp(0, Set(SynsetType)))
    } else {
      throw new WQueryStaticCheckException("{...} requires an expression that generates either senses or word forms")
    }
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    expr match {
      case RelationUnionExpr(List(QuantifiedRelationExpr(ArcExpr(List(ArcExprArgument(id, None))),Quantifier(1,Some(1))))) =>
        val sourceTypes = if (bindings.areContextVariablesBound) bindings.lookupContextVariableType(0) else DataType.all

        if (wordNet.containsRelation(id, sourceTypes, Relation.Source)) {
          extendBasedEvaluationPlan(wordNet, bindings)
        } else {
          FetchOp.wordByValue(id)
        }
      case _ =>
        extendBasedEvaluationPlan(wordNet, bindings)
    }
  }

  private def extendBasedEvaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    if (bindings.areContextVariablesBound) {
      val contextType = bindings.lookupContextVariableType(0)

      ExtendOp(ContextRefOp(0, contextType), 0, expr.evaluationPattern(wordNet, contextType))
    } else {
      val pattern = expr.evaluationPattern(wordNet, DataType.all)
      val fetches = pattern.sourceType.collect {
        case SynsetType => FetchOp.synsets
        case SenseType => FetchOp.senses
        case StringType => FetchOp.words
        case _ => ConstantOp.empty
      }

      ExtendOp(fetches.tail.foldLeft(UnionOp(fetches.head, ConstantOp.empty))((left, right) => UnionOp(left, right)) , 0, pattern)
    }
  }
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    SelectOp(FetchOp.words, BinaryConditionalExpr("=~", AlgebraExpr(ContextRefOp(0, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))).conditionPlan(wordNet, bindings))
  }
}

case class ContextReferenceReq(pos: Int) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = ContextRefOp(pos, bindings.lookupContextVariableType(pos))
}

case class BooleanByFilterReq(conditionalExpr: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    IfElseOp(SelectOp(ConstantOp.fromValue(true), conditionalExpr.conditionPlan(wordNet, bindings)),
      ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = variable match {
    case PathVariable(name) =>
      bindings.lookupPathVariableType(name).map(PathVariableRefOp(name, _))
        .getOrElse(throw new WQueryStaticCheckException("A reference to unknown variable @" + name + " found"))
    case StepVariable(name) =>
      bindings.lookupStepVariableType(name).map(StepVariableRefOp(name, _))
        .getOrElse(throw new WQueryStaticCheckException("A reference to unknown variable $" + name + " found"))
  }
}

case class ArcByArcExprReq(expr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    val pattern = expr.evaluationPattern(wordNet, DataType.all)
    ConstantOp(DataSet(pattern.destinations.map(destination => List(Arc(pattern.relation, pattern.source, destination)))))
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = op
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
