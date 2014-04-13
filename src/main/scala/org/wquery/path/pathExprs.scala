package org.wquery.path.exprs

import scalaz._
import Scalaz._
import collection.mutable.ListBuffer
import org.wquery._
import org.wquery.model._
import org.wquery.lang._
import org.wquery.lang.exprs._
import org.wquery.lang.operations._
import org.wquery.path._
import org.wquery.path.operations._
import org.wquery.query.operations.IfElseOp
import org.wquery.query.SetVariable

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
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
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
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
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
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp): AlgebraOp
}

case class RelationTransformationExpr(expr: RelationalExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val pattern = expr.evaluationPattern(wordNet, op.rightType(0))
    ExtendOp(op, pattern, VariableTemplate.empty)
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings union op.bindingsPattern, false)

    filterBindings.bindStepVariableType(StepVariable.ContextVariable.name, op.rightType(0))

    val condition = conditionalExpr.conditionPlan(wordNet, filterBindings, context.copy(creation = false))
    val filteredOp = if (condition.referencedVariables.contains(StepVariable.ContextVariable))
      BindOp(op, VariableTemplate(List(StepVariable.ContextVariable))) else op

    SimplificationRules.applyConditionIfNotRedundant(filteredOp, condition)
  }

}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    if (op /== ConstantOp.empty) {
      FilterTransformationExpr(BinaryConditionalExpr("in", ContextByVariableReq(StepVariable.ContextVariable), generator))
        .push(wordNet, bindings, context, op)
    } else {
      val generateOp = generator.evaluationPlan(wordNet, bindings, context)
      generateOp
    }
  }
}

case class BindTransformationExpr(variables: VariableTemplate) extends TransformationExpr {
  def push(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    if (variables /== ∅[VariableTemplate]) {
      op match {
        case ExtendOp(extendedOp, pattern, VariableTemplate.empty) =>
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

case class ArcExpr(ids: List[ArcExprArgument]) extends RelationalExpr {
  def creationPattern(wordNet: WordNet#Schema) = {
    if (ids.size > 1 && ids(0).nodeType.isDefined && ids.exists(_.name === Relation.Src) &&
        ids(1).nodeType.isEmpty && (ids(1).name /== Relation.AnyName) && ids.tail.tail.forall(_.nodeType.isDefined)) {
      val relation = Relation(ids(1).name,
        (Argument(ids(0).name, ids(0).nodeType.get) :: ids.tail.tail.map(elem => Argument(elem.name, elem.nodeType.get))))

      ArcRelationalPattern(ArcPattern(Some(relation),
        ArcPatternArgument(ids(0).name, ids(0).nodeType),
        ids.tail.tail.map(id => ArcPatternArgument(id.name, id.nodeType))))
    } else {
      throw new WQueryStaticCheckException("Arc expression " + toString + " does not determine a relation to create unambiguously")
    }
  }

  def evaluationPattern(wordNet: WordNet#Schema, contextTypes: Set[DataType]) = {
    ((ids: @unchecked) match {
      case List(ArcExprArgument("_", nodeType)) =>
        Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument.anyFor(nodeType.map(NodeType.fromName(_))), List(ArcPatternArgument.Any))))
      case List(arg) =>
        if (arg.nodeType.isEmpty) {
          wordNet.getRelation(arg.name, Map((Relation.Src, contextTypes)))
            .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(Relation.Src, Some(relation.sourceType)),
              relation.destinationType.map(destinationType => ArcPatternArgument(Relation.Dst, Some(destinationType))).toList)))
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

  private def evaluateAsSourceTypePattern(wordNet: WordNet#Schema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument,
                                          rest: List[ArcExprArgument]) = {
    val sourceTypes = left.nodeType.some(Set[DataType](_)).none(contextTypes)
    val sourceName = left.name
    val relationName = right.name
    val emptyDestination = rest.isEmpty??(List(ArcPatternArgument(Relation.Dst, None)).some)

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(sourceName, left.nodeType),
        emptyDestination|(rest.map(arg => ArcPatternArgument(arg.name, arg.nodeType))))))
    } else {
      wordNet.getRelation(right.name, ((sourceName, sourceTypes) +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(sourceName, Some(relation.sourceType)),
        emptyDestination|(rest.map(arg => ArcPatternArgument(arg.name, relation.demandArgument(arg.name).nodeType.some))))))
    }
  }

  private def evaluateAsDestinationTypePattern(wordNet: WordNet#Schema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument,
                                               rest: List[ArcExprArgument]) = {
    val relationName = left.name

    if (relationName === Relation.AnyName) {
      Some(ArcRelationalPattern(ArcPattern(None, ArcPatternArgument(Relation.Src, None),
        (right::rest).map(arg => ArcPatternArgument(arg.name, arg.nodeType)))))
    } else {
      wordNet.getRelation(relationName, ((Relation.Src, contextTypes) +: right.nodeDescription +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcRelationalPattern(ArcPattern(Some(relation), ArcPatternArgument(Relation.Src, Some(relation.sourceType)),
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
    val exprsOp = exprs.foldLeft[AlgebraOp](ConstantOp.empty) { (contextOp, expr) =>
      expr.push(wordNet, bindings, context, contextOp)
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
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
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
  def conditionPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = RightFringeCondition(expr.evaluationPlan(wordNet, bindings, context))
}

/*
 * Generators
 */
case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
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
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    FetchOp.senseByValue(new Sense(wordForm, senseNumber, pos))
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet#Schema, bindings: BindingsSchema, context: Context) = {
    expr match {
      case ArcExpr(List(ArcExprArgument(id, None))) =>
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