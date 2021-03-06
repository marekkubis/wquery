package org.wquery.path.exprs

import org.wquery._
import org.wquery.lang._
import org.wquery.lang.exprs._
import org.wquery.lang.operations._
import org.wquery.model._
import org.wquery.path._
import org.wquery.path.operations._
import org.wquery.query.SetVariable
import org.wquery.query.operations.IfElseOp

import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._

/*
 * Path expressions
 */
case class BinarySetExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
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
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    expr.evaluationPlan(wordNet, BindingsSchema(bindings, false), context)
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmeticExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val leftOp = left.evaluationPlan(wordNet, bindings, context)
    val rightOp = right.evaluationPlan(wordNet, bindings, context)

    if (leftOp.rightType(0).any(t => t == IntegerType || t == FloatType)
      && rightOp.rightType(0).any(t => t == IntegerType || t == FloatType)) {
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
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    if (op.rightType(0).any(t => t == IntegerType || t == FloatType))
      MinusOp(op)
    else
      throw new WQueryEvaluationException("Operator '-' requires a path that ends with float or integer values")
  }
}

/*
 * Function call expressions
 */
case class FunctionExpr(name: String, args: Option[EvaluableExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val argsOp = args.map(_.evaluationPlan(wordNet, bindings, context))

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
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp, contextExpr: Option[TransformationExpr]): AlgebraOp
}

case class RelationTransformationExpr(expr: RelationalExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp, contextExpr: Option[TransformationExpr]) = {
    val pattern = expr.evaluationPattern(wordNet, op.rightType(0))
    ExtendOp(op, pattern, VariableTemplate.empty)
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp, contextExpr: Option[TransformationExpr]) = {
    val filterBindings = BindingsSchema(bindings union op.bindingsPattern, false)

    filterBindings.bindStepVariableType(StepVariable.ContextVariable.name, op.rightType(0))

    val condition = conditionalExpr.conditionPlan(wordNet, filterBindings, context.copy(creation = false))
    val filteredOp = if (condition.referencedVariables.contains(StepVariable.ContextVariable))
      BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op

    SimplificationRules.applyConditionIfNotRedundant(filteredOp, condition)
  }

}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp, contextExpr: Option[TransformationExpr]) = {
    if (contextExpr.isDefined) {
      FilterTransformationExpr(BinaryConditionalExpr("in", ContextByVariableReq(StepVariable.ContextVariable), generator))
        .push(wordNet, bindings, context, op, contextExpr)
    } else {
      val generateOp = generator.evaluationPlan(wordNet, bindings, context)
      generateOp
    }
  }
}

case class BindTransformationExpr(variables: VariableTemplate) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp, contextExpr: Option[TransformationExpr]) = {
    if (variables /== ∅[VariableTemplate]) {
      (op, contextExpr) match {
        case (ExtendOp(extendedOp, pattern, VariableTemplate.empty), Some(RelationTransformationExpr(_))) =>
          ExtendOp(extendedOp, pattern, variables)
        case _ =>
          BindOp(op, variables)
      }
    } else {
      op
    }
  }
}

sealed abstract class RelationalExpr extends Expr {
  def evaluationPattern(wordNet: WordNet#Schema, sourceTypes: Set[DataType]): RelationalPattern
}

case class RelationUnionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNet#Schema, sourceTypes: Set[DataType]) = {
    if (exprs.size === 1)
      exprs.head.evaluationPattern(wordNet, sourceTypes)
    else
      RelationUnionPattern(exprs.map(_.evaluationPattern(wordNet, sourceTypes)))
  }
}

case class RelationCompositionExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNet#Schema, sourceTypes: Set[DataType]) = {
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
  def evaluationPattern(wordNet: WordNet#Schema, sourceTypes: Set[DataType]) = {
    if (quantifier.lowerBound >= 0 && quantifier.upperBound
      .some(upperBound => quantifier.lowerBound < upperBound || quantifier.lowerBound === upperBound && (upperBound /== 0))
      .none(true))
      QuantifiedRelationPattern(expr.evaluationPattern(wordNet, sourceTypes), quantifier)
    else
      throw new WQueryStaticCheckException("Invalid bound in quantifier " + quantifier)
  }
}

case class ArcExpr(left: ArcExprArgument, right: Option[(ArcExprArgument, Option[ArcExprArgument])]) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNet#Schema, contextTypes: Set[DataType]) = {
    ((left, right) match {
      case (ArcExprArgument("_", nodeType), None) =>
        Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument.anyFor(nodeType.map(NodeType.fromName(_))), ArcPatternArgument.Any)))
      case (arg, None) =>
        if (arg.nodeType.isEmpty) {
          val relations = wordNet.getRelations(arg.name, Map((Relation.Src, contextTypes)))

          relations.nonEmpty.option {
            val sourcePattern = ArcPatternArgument(Relation.Src, Some(relations.head.sourceType))
            val destinationPattern = if (relations.head.isTraversable) ArcPatternArgument(Relation.Dst, relations.head.destinationType) else sourcePattern

            ArcRelationalPattern(ArcPattern(Some(relations), sourcePattern, destinationPattern))
          }
        } else {
          throw new WQueryStaticCheckException("Relation name " + arg.name + " cannot be followed by type specifier &")
        }
      case (left, Some((right, rest))) =>
        if (left.nodeType.isDefined && right.nodeType.isDefined) {
          throw new WQueryStaticCheckException("No relation name found in arc expression " + toString)
        } else if (left.nodeType.isDefined) {
          evaluateAsSourceTypePattern(wordNet, contextTypes, left, right, rest)
        } else if (right.nodeType.isDefined) {
          evaluateAsDestinationTypePattern(wordNet, contextTypes, left, right)
        } else {
          evaluateAsDestinationTypePattern(wordNet, contextTypes, left, right)
            .orElse(evaluateAsSourceTypePattern(wordNet, contextTypes, left, right, rest))
        }
    })|(throw new WQueryStaticCheckException("Arc expression " + toString + " references an unknown relation or argument"))
  }

  private def evaluateAsSourceTypePattern(wordNet: WordNet#Schema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument,
                                          rest: Option[ArcExprArgument]) = {
    val sourceTypes = left.nodeType.some(Set[DataType](_)).none(contextTypes)
    val sourceName = left.name
    val relationName = right.name
    val emptyDestination = ArcPatternArgument(Relation.Dst, None)

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(sourceName, left.nodeType),
        rest.some(arg => ArcPatternArgument(arg.name, arg.nodeType)).none(emptyDestination))))
    } else {
      val relations = wordNet.getRelations(right.name, ((sourceName, sourceTypes) :: rest.map(_.nodeDescription).toList).toMap)

      relations.nonEmpty.option(ArcRelationalPattern(ArcPattern(Some(relations), ArcPatternArgument(sourceName, Some(relations.head.sourceType)),
        rest.some(arg => ArcPatternArgument(arg.name, relations.head.demandArgument(arg.name).nodeType.some)).none(emptyDestination))))
    }
  }

  private def evaluateAsDestinationTypePattern(wordNet: WordNet#Schema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument) = {
    val relationName = left.name

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(Relation.Src, None),
        ArcPatternArgument(right.name, right.nodeType))))
    } else {
      val relations = wordNet.getRelations(relationName, Map((Relation.Src, contextTypes), right.nodeDescription))

      relations.nonEmpty.option(ArcRelationalPattern(ArcPattern(Some(relations), ArcPatternArgument(Relation.Src, Some(relations.head.sourceType)),
        ArcPatternArgument(right.name, relations.head.demandArgument(right.name).nodeType.some))))
    }
  }

  override def toString = left + ~right.map{ case (a, b) => "^" + a + ~b.map("^" + _)}
}

case class ArcExprArgument(name: String, nodeTypeName: Option[String]) extends Expr {
  val nodeType: Option[NodeType] = nodeTypeName.map(n => NodeType.fromName(n))
  val nodeDescription = (name, nodeType.some(Set[DataType](_)).none(NodeType.all.toSet[DataType]))

  override def toString = name + ~nodeTypeName.map("&" + _)
}

case class ProjectionExpr(expr: EvaluableExpr) extends Expr {
  def project(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val projectionBindings = BindingsSchema(bindings union op.bindingsPattern, false)

    projectionBindings.bindStepVariableType(StepVariable.ContextVariable.name, op.rightType(0))

    val projectOp = expr.evaluationPlan(wordNet, projectionBindings, context.copy(creation = false))
    val projectedOp = if (projectOp.referencedVariables.contains(StepVariable.ContextVariable))
      BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op

    ProjectOp(projectedOp, projectOp)
  }
}

case class PathExpr(exprs: List[TransformationExpr], projections: List[ProjectionExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val (exprsOp, _) = exprs.foldLeft[(AlgebraOp, Option[TransformationExpr])]((ConstantOp.empty, None)) { case ((contextOp, contextExpr), expr) =>
      (expr.push(wordNet, bindings, context, contextOp, contextExpr), Some(expr))
    }

    projections.foldLeft[AlgebraOp](exprsOp) { (contextOp, expr) =>
      expr.project(wordNet, bindings, context, contextOp)
    }
  }
}

/*
 * Conditional Expressions
 */
sealed abstract class ConditionalExpr extends Expr {
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context): Condition
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = OrCondition(exprs.map(_.conditionPlan(wordNet, bindings, context)))
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = AndCondition(exprs.map(_.conditionPlan(wordNet, bindings, context)))
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = NotCondition(expr.conditionPlan(wordNet, bindings, context))
}

case class BinaryConditionalExpr(op: String, leftExpr: EvaluableExpr, rightExpr: EvaluableExpr) extends ConditionalExpr {
  val Operators = Set("=", "==", "===", "@=", "@==", "@===", "@!=", "@!==", "@!===", "!=", "!==", "!===", "in", "<<", "<<<", "@<<", "@<<<", "=~", "<=", "<", ">=", ">")

  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val leftOp = leftExpr.evaluationPlan(wordNet, bindings, context)
    val rightOp = rightExpr.evaluationPlan(wordNet, bindings, context)

    if (Operators.contains(op))
      BinaryCondition(op, leftOp, rightOp)
    else
      throw new IllegalArgumentException("Unknown comparison operator '" + op + "'")
  }
}

case class PathConditionExpr(expr: EvaluableExpr) extends ConditionalExpr {
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = RightFringeCondition(expr.evaluationPlan(wordNet, bindings, context))
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)
    val contextTypes = op.rightType(0).filter(t => t === StringType || t === SenseType)

    if (contextTypes.nonEmpty) {
      SynsetFetchOp(op)
    } else {
      throw new WQueryStaticCheckException("{...} requires an expression that generates either senses or word forms")
    }
  }
}

case class SenseByWordFormAndSenseNumberAndPosReq(wordForm: String, senseNumber:Int, pos: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    FetchOp.senseByValue(new Sense(wordForm, senseNumber, pos))
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    expr match {
      case ArcExpr(ArcExprArgument(id, None), None) =>
        val sourceTypes = bindings.lookupStepVariableType(StepVariable.ContextVariable.name)|DataType.all

        if (wordNet.containsRelation(id, Map((Relation.Src, sourceTypes))) || id === Relation.AnyName) {
          extendBasedEvaluationPlan(wordNet, bindings)
        } else {
          if (context.creation) ConstantOp.fromValue(id) else FetchOp.wordByValue(id)
        }
      case _ =>
        extendBasedEvaluationPlan(wordNet, bindings)
    }
  }

  private def extendBasedEvaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema) = {
    bindings.lookupStepVariableType(StepVariable.ContextVariable.name).map{ contextType =>
      ExtendOp(StepVariableRefOp(StepVariable.ContextVariable, contextType), expr.evaluationPattern(wordNet, contextType), VariableTemplate.empty)
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
        ExtendOp(fetchOp, pattern, VariableTemplate.empty)
    }
  }
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val filteredVariable = StepVariable("__r")

    SimplificationRules.applyConditionIfNotRedundant(BindOp(FetchOp.words, VariableTemplate(List(filteredVariable))),
      BinaryConditionalExpr("=~", AlgebraExpr(StepVariableRefOp(filteredVariable, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))).conditionPlan(wordNet, bindings, context))
  }
}

case class BooleanByFilterReq(conditionalExpr: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    IfElseOp(SimplificationRules.applyConditionIfNotRedundant(ConstantOp.fromValue(true), conditionalExpr.conditionPlan(wordNet, bindings, context)),
      ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = variable match {
    case variable @ SetVariable(name) =>
      bindings.lookupSetVariableType(name).some(SetVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))
    case variable @ TupleVariable(name) =>
      bindings.lookupTupleVariableType(name).some(PathVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))
    case variable @ StepVariable(name) =>
      bindings.lookupStepVariableType(name).some(StepVariableRefOp(variable, _))
        .none(throw new FoundReferenceToUnknownVariableWhileCheckingException(variable))

  }
}

case class ArcByArcExprReq(expr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val relationalPattern = expr.evaluationPattern(wordNet, DataType.all)
    val pattern = relationalPattern.pattern

    pattern.relations.some { relations =>
      ConstantOp(DataSet.fromList(relations.map(relation => Arc(relation, pattern.source.name, pattern.destination.name))))
    }.none {
      val toMap = pattern.destination match {
        case ArcPatternArgument.Any =>
          ∅[Map[String, Option[NodeType]]]
        case dest =>
          Map((dest.name, dest.nodeType))
      }

      val arcs = for (relation <- wordNet.relations;
                      source <- relation.argumentNames if ((pattern.source.name === ArcPatternArgument.AnyName && source === Relation.Src) ||
          pattern.source.name === source) && pattern.source.nodeType.some(_ === relation.demandArgument(source).nodeType).none(true);
                      destination <- relation.argumentNames if (toMap.isEmpty && destination === Relation.Dst) || toMap.get(destination).some(nodeTypeOption =>
          nodeTypeOption.some(_ === relation.demandArgument(destination).nodeType).none(true)).none(false)
                      if relation.arguments.size === 1 || (source /== destination))
      yield List(Arc(relation, source, destination))

      ConstantOp(DataSet(arcs))
    }
  }
}

case class DomainReq() extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val domainSetFetches =  List(FetchOp.synsets, FetchOp.senses, FetchOp.words, FetchOp.possyms)
    val relationArgumentFetches = (for (relation <- wordNet.relations; argument <- relation.arguments
         if argument.nodeType != SynsetType &&  argument.nodeType != SenseType)
      yield FetchOp(relation, List((argument.name, Nil)), List(argument.name)))

    (domainSetFetches ++ relationArgumentFetches).foldLeft[AlgebraOp](ConstantOp.empty)((l, r) => UnionOp(l, r))
  }
}

case class RelationByNameReq(name: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    val relation = wordNet.demandRelation(name, Map())
    FetchOp(relation, relation.argumentNames.map(arg => (arg, Nil)), relation.argumentNames)
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = op
}
