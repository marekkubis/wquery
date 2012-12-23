// scalastyle:off number.of.types

package org.wquery.engine

import planner.{ConditionApplier, PlanEvaluator, PathPlanGenerator, PathBuilder}
import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.engine.operations._
import org.wquery.{FoundReferenceToUnknownVariableWhileCheckingException, WQueryStaticCheckException, WQueryEvaluationException}
import collection.mutable.ListBuffer

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context): AlgebraOp
}

/*
 * Imperative expressions
 */
case class EmissionExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = EmitOp(expr.evaluationPlan(wordNet, bindings, context))
}

case class IteratorExpr(bindingExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val bindingsOp = bindingExpr.evaluationPlan(wordNet, bindings, context)
    IterateOp(bindingsOp, iteratedExpr.evaluationPlan(wordNet, bindings union bindingsOp.bindingsPattern, context))
  }
}

case class IfElseExpr(conditionExpr: EvaluableExpr, ifExpr: EvaluableExpr, elseExpr: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = IfElseOp(conditionExpr.evaluationPlan(wordNet, bindings, context),
    ifExpr.evaluationPlan(wordNet, bindings, context), elseExpr.map(_.evaluationPlan(wordNet, bindings, context)))
}

case class BlockExpr(exprs: List[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val blockBindings = BindingsSchema(bindings, true)

    BlockOp(exprs.map(expr => expr.evaluationPlan(wordNet, blockBindings, context)))
  }
}

case class WhileDoExpr(conditionExpr: EvaluableExpr, iteratedExpr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context)
    = WhileDoOp(conditionExpr.evaluationPlan(wordNet, bindings, context), iteratedExpr.evaluationPlan(wordNet, bindings, context))
}

case class MergeExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    if (op.rightType(0).subsetOf(Set(SynsetType, SenseType))) {
      MergeOp(op)
    } else {
      throw new WQueryStaticCheckException("Merge operation requires synsets and/or senses as arguments")
    }
  }
}

case class VariableAssignmentExpr(variable: SetVariable, expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)
    bindings.bindSetVariableType(variable.name, op)
    AssignmentOp(variable, op)
  }
}

case class RelationAssignmentExpr(name: String, expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val pattern = expr.evaluationPattern(wordNet, DataType.all)
    val sourceType = demandSingleNodeType(pattern.sourceType, "source")
    val destinationType = demandSingleNodeType(pattern.rightType(0), "destination")

    CreateRelationFromPatternOp(name, pattern, sourceType, destinationType)
  }

  private def demandSingleNodeType(types: Set[_ <: DataType], typeName: String) = {
    if (types.size === 1 && types.head.isInstanceOf[NodeType])
      types.head.asInstanceOf[NodeType]
    else
      throw new WQueryEvaluationException("Expression " + expr + " does not determine single " + typeName + " type")
  }
}

case class UpdateExpr(left: Option[EvaluableExpr], spec: RelationSpecification, op: String, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val leftOp = left.map(_.evaluationPlan(wordNet, bindings, context))
    val rightOp = right.evaluationPlan(wordNet, bindings, context)

    op match {
      case "+=" =>
        AddTuplesOp(leftOp, spec, rightOp)
      case "-=" =>
        RemoveTuplesOp(leftOp, spec, rightOp)
      case ":=" =>
        SetTuplesOp(leftOp, spec, rightOp)
    }
  }
}

/*
 * Path expressions
 */
case class BinarySetExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val leftPlan = left.evaluationPlan(wordNet, bindings, context)
    val rightPlan = right.evaluationPlan(wordNet, bindings, context)

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
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    expr.evaluationPlan(wordNet, BindingsSchema(bindings, false), context)
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmeticExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val leftOp = left.evaluationPlan(wordNet, bindings, context)
    val rightOp = right.evaluationPlan(wordNet, bindings, context)

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
        case "mod" =>
          ModOp(leftOp, rightOp)
      }
    } else {
      throw new WQueryEvaluationException("Operator '" + op + "' requires paths that end with float or integer values")
    }
  }
}

case class MinusExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

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
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val argsOp = args.evaluationPlan(wordNet, bindings, context)

    Functions.findFunctionsByName(name).some { functions =>
      functions.find(_.accepts(argsOp)).some(FunctionOp(_, argsOp))
        .none(throw new WQueryEvaluationException("Function '" + name + "' cannot accept provided arguments"))
    }.none(throw new WQueryEvaluationException("Function '" + name + "' not found"))
  }
}

/*
 * Step related expressions
 */
sealed abstract class TransformationExpr extends Expr {
  def push(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp, plan: PathBuilder): AlgebraOp
}

case class RelationTransformationExpr(expr: RelationalExpr) extends TransformationExpr {
  def push(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp, plan: PathBuilder) = {
    val pattern = expr.evaluationPattern(wordNet, op.rightType(0))
    plan.appendLink(pattern)
    ExtendOp(op, pattern, Forward, VariableTemplate.empty)
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def push(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp, plan: PathBuilder) = {
    val filterBindings = BindingsSchema(bindings union op.bindingsPattern, false)

    filterBindings.bindStepVariableType(StepVariable.ContextVariable.name, op.rightType(0))

    val condition = conditionalExpr.conditionPlan(wordNet, filterBindings, context.copy(creation = false))
    val filteredOp = if (condition.referencedVariables.contains(StepVariable.ContextVariable))
      BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op

    plan.appendCondition(condition)
    ConditionApplier.applyIfNotRedundant(filteredOp, condition)
  }
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def push(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp, plan: PathBuilder) = {
    if (op /== ConstantOp.empty) {
      FilterTransformationExpr(BinaryConditionalExpr("in", ContextByVariableReq(StepVariable.ContextVariable), generator))
        .push(wordNet, bindings, context, op, plan)
    } else {
      val generateOp = generator.evaluationPlan(wordNet, bindings, context)
      plan.createRootLink(generateOp)
      generateOp
    }
  }
}

case class BindTransformationExpr(variables: VariableTemplate) extends TransformationExpr {
  def push(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp, plan: PathBuilder) = {
    if (variables /== ∅[VariableTemplate]) {
      plan.appendVariables(variables)

      op match {
        case ExtendOp(extendedOp, pattern, direction, VariableTemplate.empty) =>
          ExtendOp(extendedOp, pattern, direction, variables)
        case _ =>
          BindOp(op, variables)
      }
    } else {
      op
    }
  }
}

sealed abstract class RelationalExpr extends Expr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]): RelationalPattern
}

case class RelationUnionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    if (exprs.size === 1)
      exprs.head.evaluationPattern(wordNet, sourceTypes)
    else
      RelationUnionPattern(exprs.map(_.evaluationPattern(wordNet, sourceTypes)))
  }
}

case class RelationCompositionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    val headPattern = exprs.head.evaluationPattern(wordNet, sourceTypes)

    if (exprs.size === 1) {
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
    if (quantifier.lowerBound >= 0 && quantifier.upperBound
      .some(upperBound => quantifier.lowerBound < upperBound || quantifier.lowerBound === upperBound && (upperBound /== 0))
      .none(true))
      QuantifiedRelationPattern(expr.evaluationPattern(wordNet, sourceTypes), quantifier)
    else
      throw new WQueryStaticCheckException("Invalid bound in quantifier " + quantifier)
  }
}

case class ArcExpr(ids: List[ArcExprArgument]) extends RelationalExpr {
  def creationPattern(wordNet: WordNetSchema) = {
    if (ids.size > 1 && ids(0).nodeType.isDefined && ids.exists(_.name === Relation.Source) &&
        ids(1).nodeType.isEmpty && (ids(1).name /== Relation.AnyName) && ids.tail.tail.forall(_.nodeType.isDefined)) {
      val relation = Relation(ids(1).name,
        (Argument(ids(0).name, ids(0).nodeType.get) :: ids.tail.tail.map(elem => Argument(elem.name, elem.nodeType.get))).toSet)

      ArcRelationalPattern(ArcPattern(Some(relation),
        ArcPatternArgument(ids(0).name, ids(0).nodeType),
        ids.tail.tail.map(id => ArcPatternArgument(id.name, id.nodeType))))
    } else {
      throw new WQueryStaticCheckException("Arc expression " + toString + " does not determine a relation to create unambiguously")
    }
  }

  def evaluationPattern(wordNet: WordNetSchema, contextTypes: Set[DataType]) = {
    ((ids: @unchecked) match {
      case List(ArcExprArgument("_", nodeType)) =>
        Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument.anyFor(nodeType.map(NodeType.fromName(_))), List(ArcPatternArgument.Any))))
      case List(arg) =>
        if (arg.nodeType.isEmpty) {
          wordNet.getRelation(arg.name, Map((Relation.Source, contextTypes)))
            .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(Relation.Source, Some(relation.sourceType)),
              relation.destinationType.map(destinationType => ArcPatternArgument(Relation.Destination, Some(destinationType))).toList)))
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
    })|(throw new WQueryStaticCheckException("Arc expression " + toString + " references an unknown relation or argument"))
  }

  private def evaluateAsSourceTypePattern(wordNet: WordNetSchema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument,
                                          rest: List[ArcExprArgument]) = {
    val sourceTypes = left.nodeType.some(Set[DataType](_)).none(contextTypes)
    val sourceName = left.name
    val relationName = right.name
    val emptyDestination = rest.isEmpty??(List(ArcPatternArgument(Relation.Destination, None)).some)

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(sourceName, left.nodeType),
        emptyDestination|(rest.map(arg => ArcPatternArgument(arg.name, arg.nodeType))))))
    } else {
      wordNet.getRelation(right.name, ((sourceName, sourceTypes) +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(sourceName, Some(relation.sourceType)),
        emptyDestination|(rest.map(arg => ArcPatternArgument(arg.name, relation.demandArgument(arg.name).nodeType.some))))))
    }
  }

  private def evaluateAsDestinationTypePattern(wordNet: WordNetSchema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument,
                                               rest: List[ArcExprArgument]) = {
    val relationName = left.name

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(Relation.Source, None),
        (right::rest).map(arg => ArcPatternArgument(arg.name, arg.nodeType)))))
    } else {
      wordNet.getRelation(relationName, ((Relation.Source, contextTypes) +: right.nodeDescription +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(Relation.Source, Some(relation.sourceType)),
          (right::rest).map(arg => ArcPatternArgument(arg.name, relation.demandArgument(arg.name).nodeType.some)))))
    }
  }

  override def toString = ids.mkString("^")
}

case class ArcExprArgument(name: String, nodeTypeName: Option[String]) extends Expr {
  val nodeType: Option[NodeType] = nodeTypeName.map(n => NodeType.fromName(n))
  val nodeDescription = (name, nodeType.some(Set[DataType](_)).none(NodeType.all.toSet[DataType]))

  override def toString = name + ~nodeTypeName.map("&" + _)
}

case class ProjectionExpr(expr: EvaluableExpr) extends Expr {
  def project(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val projectionBindings = BindingsSchema(bindings union op.bindingsPattern, false)

    projectionBindings.bindStepVariableType(StepVariable.ContextVariable.name, op.rightType(0))

    val projectOp = expr.evaluationPlan(wordNet, projectionBindings, context.copy(creation = false))
    val projectedOp = if (projectOp.referencedVariables.contains(StepVariable.ContextVariable))
      BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op

    ProjectOp(projectedOp, projectOp)
  }
}

case class PathExpr(exprs: List[TransformationExpr], projections: List[ProjectionExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val path = createPath(exprs, wordNet, bindings, context)
    val plans = new PathPlanGenerator(path).plan(wordNet, bindings)
    val plan = PlanEvaluator.chooseBest(wordNet, plans)

    projections.foldLeft[AlgebraOp](plan) { (contextOp, expr) =>
      expr.project(wordNet, bindings, context, contextOp)
    }
  }

  def createPath(exprs: List[TransformationExpr], wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val builder = new PathBuilder

    exprs.foldLeft[AlgebraOp](ConstantOp.empty) { (contextOp, expr) =>
      expr.push(wordNet, bindings, context, contextOp, builder)
    }

    builder.build
  }
}

/*
 * Conditional Expressions
 */
sealed abstract class ConditionalExpr extends Expr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context): Condition
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = OrCondition(exprs.map(_.conditionPlan(wordNet, bindings, context)))
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = AndCondition(exprs.map(_.conditionPlan(wordNet, bindings, context)))
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = NotCondition(expr.conditionPlan(wordNet, bindings, context))
}

case class BinaryConditionalExpr(op: String, leftExpr: EvaluableExpr, rightExpr: EvaluableExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val leftOp = leftExpr.evaluationPlan(wordNet, bindings, context)
    val rightOp = rightExpr.evaluationPlan(wordNet, bindings, context)

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

case class PathConditionExpr(expr: EvaluableExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = RightFringeCondition(expr.evaluationPlan(wordNet, bindings, context))
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)
    val contextTypes = op.rightType(0).filter(t => t === StringType || t === SenseType)

    if (!contextTypes.isEmpty) {
      SynsetFetchOp(op)
    } else {
      throw new WQueryStaticCheckException("{...} requires an expression that generates either senses or word forms")
    }
  }
}

case class SenseByWordFormAndSenseNumberAndPosReq(wordForm: String, senseNumber:Int, pos: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    FetchOp.senseByValue(new Sense(wordForm, senseNumber, pos))
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    expr match {
      case ArcExpr(List(ArcExprArgument(id, None))) =>
        val sourceTypes = bindings.lookupStepVariableType(StepVariable.ContextVariable.name)|DataType.all

        if (wordNet.containsRelation(id, Map((Relation.Source, sourceTypes))) || id === Relation.AnyName) {
          extendBasedEvaluationPlan(wordNet, bindings)
        } else {
          if (context.creation) ConstantOp.fromValue(id) else FetchOp.wordByValue(id)
        }
      case _ =>
        extendBasedEvaluationPlan(wordNet, bindings)
    }
  }

  private def extendBasedEvaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    bindings.lookupStepVariableType(StepVariable.ContextVariable.name).map{ contextType =>
      ExtendOp(StepVariableRefOp(StepVariable.ContextVariable, contextType), expr.evaluationPattern(wordNet, contextType), Forward, VariableTemplate.empty)
    }.getOrElse {
      val pattern = expr.evaluationPattern(wordNet, DataType.all)

      val fetches = pattern.sourceType.collect {
        case SynsetType => FetchOp.synsets
        case SenseType => FetchOp.senses
        case StringType => FetchOp.words
        case POSType => FetchOp.possyms
      }

      val fetchOp = fetches.reduceLeft[AlgebraOp]{ case (left, right) => UnionOp(left, right) }

      if (pattern.minTupleSize === 0 && pattern.maxTupleSize === Some(0))
        fetchOp
      else
        ExtendOp(fetchOp, pattern, Forward, VariableTemplate.empty)
    }
  }
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val filteredVariable = StepVariable("__r")

    ConditionApplier.applyIfNotRedundant(BindOp(FetchOp.words, VariableTemplate(List(filteredVariable))),
      BinaryConditionalExpr("=~", AlgebraExpr(StepVariableRefOp(filteredVariable, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))).conditionPlan(wordNet, bindings, context))
  }
}

case class BooleanByFilterReq(conditionalExpr: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    IfElseOp(ConditionApplier.applyIfNotRedundant(ConstantOp.fromValue(true), conditionalExpr.conditionPlan(wordNet, bindings, context)),
      ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = variable match {
    case variable @ SetVariable(name) =>
      bindings.lookupSetVariableType(name).some(SetVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))
    case variable @ TupleVariable(name) =>
      bindings.lookupPathVariableType(name).some(PathVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))
    case variable @ StepVariable(name) =>
      bindings.lookupStepVariableType(name).some(StepVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))

  }
}

case class ArcByArcExprReq(expr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val relationalPattern = if (context.creation) expr.creationPattern(wordNet) else expr.evaluationPattern(wordNet, DataType.all)
    val pattern = relationalPattern.pattern

    pattern.relation.some { relation =>
      val arcs = pattern.destinations.map(destination => List(Arc(pattern.relation.get, pattern.source.name, destination.name)))
      ConstantOp(DataSet(if (arcs.isEmpty) List(List(Arc(pattern.relation.get, pattern.source.name, pattern.source.name))) else arcs))
    }.none {
      val toMap = pattern.destinations match {
        case List(ArcPatternArgument.Any) =>
          ∅[Map[String, Option[NodeType]]]
        case _ =>
          pattern.destinations.map(arg => (arg.name, arg.nodeType)).toMap
      }

      val arcs = (for (relation <- wordNet.relations;
           source <- relation.argumentNames if (pattern.source.name === ArcPatternArgument.AnyName ||
             pattern.source.name === source)  && pattern.source.nodeType.some(_ === relation.demandArgument(source).nodeType).none(true);
           destination <- relation.argumentNames if toMap.isEmpty || toMap.get(destination).some(nodeTypeOption =>
             nodeTypeOption.some(_ === relation.demandArgument(destination).nodeType).none(true)).none(false)
           if relation.arguments.size === 1 || (source /== destination))
        yield List(Arc(relation, source, destination)))

      ConstantOp(DataSet(arcs))
    }
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = op
}
