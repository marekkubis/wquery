package org.wquery.engine
import scala.collection.mutable.ListBuffer
import org.wquery.model._
import org.wquery.{WQueryStaticCheckException, WQueryEvaluationException}

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

case class MergeExpr(expr: EvaluableExpr, withs: List[PropertyAssignmentExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    if (op.rightType(0).subsetOf(Set(SynsetType, SenseType))) {
      val patterns = withs.map(_.evaluationPattern(wordNet, bindings, context, SynsetType))

      if (patterns.exists(!_.pattern.relation.isDefined))
        throw new WQueryStaticCheckException("_ cannot be used as a relation name in the update operation")

      MergeOp(op, patterns)
    } else {
      throw new WQueryStaticCheckException("Merge operation requires synsets and/or senses as arguments")
    }
  }
}

case class SplitExpr(expr: EvaluableExpr, withs: List[PropertyAssignmentExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    if (op.rightType(0) == Set(SynsetType)) {
      val patterns = withs.map(_.evaluationPattern(wordNet, bindings, context, SynsetType))

      if (patterns.exists(!_.pattern.relation.isDefined))
        throw new WQueryStaticCheckException("_ cannot be used as a relation name in the update operation")

      SplitOp(op, patterns)
    } else {
      throw new WQueryStaticCheckException("Split operation requires synsets as arguments")
    }
  }
}

case class VariableAssignmentExpr(variables: List[Variable], expr: EvaluableExpr) extends EvaluableExpr with VariableTypeBindings {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val op = expr.evaluationPlan(wordNet, bindings, context)

    bindTypes(bindings, op, variables)
    AssignmentOp(variables, op)
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
    if (types.size == 1 && types.head.isInstanceOf[NodeType])
      types.head.asInstanceOf[NodeType]
    else
      throw new WQueryEvaluationException("Expression " + expr + " does not determine single " + typeName + " type")
  }
}

case class WordNetUpdateExpr(property: String, op: String, valuesExpr: EvaluableExpr, withs: List[PropertyAssignmentExpr]) extends EvaluableExpr {
  val Senses = "senses"
  val Synsets = "synsets"
  val Words = "words"
  val PosSymbols = "possyms"
  val Relations = "relations"

  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val valuesContext = if (op == "-=") context else context.copy(creation = true)
    val valuesOp = valuesExpr.evaluationPlan(wordNet, bindings, valuesContext)

    val contextType = property match {
      case Senses =>
        if (op == "+=") SynsetType else SenseType
      case Synsets =>
        SynsetType
      case Words =>
        StringType
      case PosSymbols =>
        StringType
      case Relations =>
        ArcType
      case property =>
        throw new WQueryStaticCheckException("wordnet has no property '" + property + "'")
    }

    checkTypes(valuesOp.rightType(0))

    val patterns = withs.map(_.evaluationPattern(wordNet, bindings, context, contextType))

    op match {
      case "+=" =>
        property match {
          case Senses =>
            AddSensesOp(valuesOp, patterns)
          case Synsets =>
            AddSynsetsOp(valuesOp, patterns)
          case Words =>
            AddWordsOp(valuesOp, patterns)
          case PosSymbols =>
            AddPartOfSpeechSymbolsOp(valuesOp, patterns)
          case Relations =>
            AddRelationsOp(valuesOp, patterns)
        }
      case ":=" =>
        property match {
          case Senses =>
            SetSensesOp(valuesOp, patterns)
          case Synsets =>
            SetSynsetsOp(valuesOp, patterns)
          case Words =>
            SetWordsOp(valuesOp, patterns)
          case PosSymbols =>
            SetPartOfSpeechSymbolsOp(valuesOp, patterns)
          case Relations =>
            SetRelationsOp(valuesOp, patterns)
        }
      case "-=" =>
        property match {
          case Senses =>
            RemoveSensesOp(valuesOp, patterns)
          case Synsets =>
            RemoveSynsetsOp(valuesOp, patterns)
          case Words =>
            RemoveWordsOp(valuesOp, patterns)
          case PosSymbols =>
            RemovePartOfSpeechSymbolsOp(valuesOp, patterns)
          case Relations =>
            RemoveRelationsOp(valuesOp, patterns)
        }
    }
  }

  private def checkTypes(valuesType: Set[DataType]) {
    if (valuesType.size == 1) {
       (property, valuesType.toList.head) match {
        case (Senses, SenseType) =>
          return
        case (Synsets, SynsetType) =>
          return
        case (Words, StringType) =>
          return
        case (PosSymbols, StringType) =>
          return
        case (Relations, ArcType) =>
          return
        case _ =>
          // continue
      }
    }

    throw new WQueryStaticCheckException("Right hand side of operator " + op + " contains values incompatibile with property "  + property)
  }
}

case class UpdateExpr(left: EvaluableExpr, arcExpr: ArcExpr, op: String, right: EvaluableExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val leftOp = left.evaluationPlan(wordNet, bindings, context)
    val leftType = leftOp.rightType(0)

    if (leftType == Set(ArcType)) {
      arcExpr match {
        case ArcExpr(List(ArcExprArgument(name, None))) =>
          val (property, action) = if (name.endsWith("_action")) (name.dropRight(7), true) else (name, false)

          if (Relation.Properties.contains(property)) {
            val rightOp = right.evaluationPlan(wordNet, bindings, context.copy(creation = true))

            if (((property == Relation.Symmetry || action) && rightOp.rightType(0) == Set(StringType)) || rightOp.rightType(0) == Set(BooleanType)) {
              RelationUpdateOp(leftOp, op, property, action, rightOp)
            } else {
              throw new WQueryStaticCheckException("Invalid relation property value on the right hand side of operator " + op)
            }
          } else {
            throw new WQueryStaticCheckException("Invalid relation property value on the right hand side of operator " + op)
          }
        case _ =>
          throw new WQueryStaticCheckException("Invalid relation property on the left hand side of operator " + op)
      }
    } else {
      val rightOp = right.evaluationPlan(wordNet, bindings, context)
      val pattern = arcExpr.evaluationPattern(wordNet, leftType)

      if (pattern.relation.isDefined) {
        op match {
          case "+=" =>
            AddTuplesOp(leftOp, pattern, rightOp)
          case "-=" =>
            RemoveTuplesOp(leftOp, pattern, rightOp)
          case ":=" =>
            SetTuplesOp(leftOp, pattern, rightOp)
        }
      } else {
        throw new WQueryStaticCheckException("_ cannot be used as a relation name in the update operation")
      }
    }
  }
}

case class PropertyAssignmentExpr(arcExpr: ArcExpr, op: String, expr: EvaluableExpr) {
  def evaluationPattern(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, contextType: DataType) = {
    val pattern = arcExpr.evaluationPattern(wordNet, Set(contextType))
    val valuesOp = expr.evaluationPlan(wordNet, bindings, context)

    for (i <- 0 until pattern.destinations.size) {
      if (pattern.leftType(2*i + 1) != valuesOp.rightType(i))
        throw new WQueryStaticCheckException("Arc pattern " + pattern + " does not match the types of values on the right hand side of " + op + " operator")
    }

    if (!pattern.relation.isDefined)
      throw new WQueryStaticCheckException("_ cannot be used as a relation name in the update operation")

    PropertyAssignmentPattern(pattern, op, valuesOp)
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
        case "%" =>
          ModOp(leftOp, rightOp)
      }
    } else {
      throw new WQueryEvaluationException("Operator '" + op +"' requires paths that end with float or integer values")
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

sealed abstract class TransformationExpr extends Expr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp): (AlgebraOp, Step)
}

case class RelationTransformationExpr(pos: Int, expr: RelationalExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val pattern = expr.evaluationPattern(wordNet, op.rightType(pos))
    (ExtendOp(op, pos, pattern), RelationStep(pos, pattern))
  }
}

case class FilterTransformationExpr(conditionalExpr: ConditionalExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val filterBindings = BindingsSchema(bindings, false)
    filterBindings.bindContextOp(op)
    val condition = conditionalExpr.conditionPlan(wordNet, filterBindings, context.copy(creation = false))
    (SelectOp(op, condition), FilterStep(condition))
  }
}

case class NodeTransformationExpr(generator: EvaluableExpr) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    if (op != ConstantOp.empty) {
      val filterBindings = BindingsSchema(bindings, false)
      filterBindings.bindContextOp(op)
      val generateOp = generator.evaluationPlan(wordNet, filterBindings, context)
      (SelectOp(op, BinaryCondition("in", ContextRefOp(0, op.rightType(0)), generateOp)), NodeStep(generateOp))
    } else {
      val generateOp = generator.evaluationPlan(wordNet, bindings, context)
      (generateOp, NodeStep(generateOp))
    }
  }
}

case class BindTransformationExpr(variables: List[Variable]) extends TransformationExpr {
  def step(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    (BindOp(op, variables), BindStep(variables))
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

case class VariableRelationalExpr(variable: StepVariable) extends RelationalExpr {
  def evaluationPattern(wordNet: WordNetSchema, sourceTypes: Set[DataType]) = {
    VariableRelationalPattern(variable)
  }
}

case class ArcExpr(ids: List[ArcExprArgument]) extends RelationalExpr {
  def creationPattern(wordNet: WordNetSchema) = {
    if (ids.size > 1 && ids(0).nodeType.isDefined && ids.exists(_.name == Relation.Source) && ids(1).nodeType.isEmpty && ids(1) != "_" && ids.tail.tail.forall(_.nodeType.isDefined)) {
      val relation = Relation(ids(1).name,(Argument(ids(0).name, ids(0).nodeType.get) :: ids.tail.tail.map(elem => Argument(elem.name, elem.nodeType.get))).toSet)

      ArcPattern(Some(relation), ArcPatternArgument(ids(0).name, ids(0).nodeType), ids.tail.tail.map(id => ArcPatternArgument(id.name, id.nodeType)))
    } else {
      throw new WQueryStaticCheckException("Arc expression " + toString + " does not determine a relation to create unambiguously")
    }
  }

  def evaluationPattern(wordNet: WordNetSchema, contextTypes: Set[DataType]) = {
    ((ids: @unchecked) match {
      case List(ArcExprArgument("_", nodeType)) =>
        Some(ArcPattern(None, ArcPatternArgument("_", nodeType.map(NodeType.fromName(_))), Nil))
      case List(arg) =>
        if (arg.nodeType.isEmpty) {
          wordNet.getRelation(arg.name, Map((Relation.Source, contextTypes)))
            .map(relation => ArcPattern(Some(relation), ArcPatternArgument(Relation.Source, Some(relation.sourceType)),
              relation.destinationType.map(destinationType => List(ArcPatternArgument(Relation.Destination, Some(destinationType)))).getOrElse(Nil)))
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
    val sourceName = left.name
    val relationName = right.name
    val destinations = if (rest.isEmpty) List(ArcPatternArgument(Relation.Destination, None)) else rest.map(arg => ArcPatternArgument(arg.name, arg.nodeType))

    if (relationName == "_") {
      Some(ArcPattern(None, ArcPatternArgument(sourceName, left.nodeType), destinations))
    } else {
      wordNet.getRelation(right.name, ((sourceName, sourceTypes) +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcPattern(Some(relation), ArcPatternArgument(sourceName, Some(relation.sourceType)), destinations))
    }
  }

  private def evaluateAsDestinationTypePattern(wordNet: WordNetSchema, contextTypes: Set[DataType], left: ArcExprArgument, right: ArcExprArgument, rest: List[ArcExprArgument]) = {
    val relationName = left.name
    val destinations = (right::rest).map(arg => ArcPatternArgument(arg.name, arg.nodeType))

    if (relationName == "_") {
      Some(ArcPattern(None, ArcPatternArgument(Relation.Source, None), destinations))
    } else {
      wordNet.getRelation(relationName, ((Relation.Source, contextTypes) +: right.nodeDescription +: (rest.map(_.nodeDescription))).toMap)
        .map(relation => ArcPattern(Some(relation), ArcPatternArgument(Relation.Source, Some(relation.sourceType)), destinations))
    }
  }

  override def toString = ids.mkString("^")
}

case class ArcExprArgument(name: String, nodeTypeName: Option[String]) extends Expr {
  val nodeType: Option[NodeType] = nodeTypeName.map(n => NodeType.fromName(n))
  val nodeDescription = (name, nodeType.map(Set[DataType](_)).getOrElse(NodeType.all.toSet[DataType]))

  override def toString = name + nodeTypeName.map("&" + _).getOrElse("")
}

case class ProjectionExpr(expr: EvaluableExpr) extends Expr {
  def project(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context, op: AlgebraOp) = {
    val projectionBindings = BindingsSchema(bindings, false)
    projectionBindings.bindContextOp(op)
    val projectOp = expr.evaluationPlan(wordNet, projectionBindings, context.copy(creation = false))
    ProjectOp(op, projectOp)
  }
}

case class PathExpr(exprs: List[TransformationExpr], projections: List[ProjectionExpr]) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val buffer = new ListBuffer[Step]

    exprs.foldLeft[AlgebraOp](ConstantOp.empty) { (contextOp, expr) =>
      expr.step(wordNet, bindings, context, contextOp) match {
        case (op, step) =>
          buffer.append(step)
          op
      }
    }

    val plannedOp = PathExprPlanner.plan(buffer.toList, wordNet, bindings)

    projections.foldLeft[AlgebraOp](plannedOp) { (contextOp, expr) =>
      expr.project(wordNet, bindings, context, contextOp)
    }
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

    if (context.creation && op.rightType(0) == Set(SenseType)) {
      NewSynsetOp(op)
    } else {
      val contextTypes = op.rightType(0).filter(t => t == StringType || t == SenseType)

      if (!contextTypes.isEmpty) {
        val patterns = contextTypes.map{ contextType => (contextType: @unchecked) match {
          case StringType =>
            ArcPattern(Some(WordNet.WordFormToSynsets), ArcPatternArgument(Relation.Source, Some(WordNet.WordFormToSynsets.sourceType)),
              List(ArcPatternArgument(Relation.Destination, WordNet.WordFormToSynsets.destinationType)))
          case SenseType =>
            ArcPattern(Some(WordNet.SenseToSynset), ArcPatternArgument(Relation.Source, Some(WordNet.SenseToSynset.sourceType)),
              List(ArcPatternArgument(Relation.Destination, WordNet.SenseToSynset.destinationType)))
        }}.toList

        ProjectOp(ExtendOp(op, 0, RelationUnionPattern(patterns)), ContextRefOp(0, Set(SynsetType)))
      } else {
        throw new WQueryStaticCheckException("{...} requires an expression that generates either senses or word forms")
      }
    }
  }
}

case class SenseByWordFormAndSenseNumberAndPosReq(wordForm: String, senseNumber:Int, pos: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    if (context.creation)
      ConstantOp.fromValue(new Sense(wordForm, senseNumber, pos))
    else
      FetchOp.senseByWordFormAndSenseNumberAndPos(wordForm, senseNumber, pos)
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    expr match {
      case RelationUnionExpr(List(QuantifiedRelationExpr(ArcExpr(List(ArcExprArgument(id, None))),Quantifier(1,Some(1))))) =>
        val sourceTypes = if (bindings.areContextVariablesBound) bindings.lookupContextVariableType(0) else DataType.all

        if (wordNet.containsRelation(id, Map((Relation.Source, sourceTypes))) || id == "_") {
          extendBasedEvaluationPlan(wordNet, bindings)
        } else {
          if (context.creation) ConstantOp.fromValue(id) else FetchOp.wordByValue(id)
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
        case POSType => FetchOp.possyms
        case _ => ConstantOp.empty
      }
      val fetchOp = fetches.tail.foldLeft(UnionOp(fetches.head, ConstantOp.empty))((left, right) => UnionOp(left, right))

      if (pattern.minSize == 0 && pattern.maxSize == Some(0))
        fetchOp
      else
        ExtendOp(fetchOp , 0, pattern)
    }
  }
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    SelectOp(FetchOp.words, BinaryConditionalExpr("=~", AlgebraExpr(ContextRefOp(0, Set(StringType))),
      AlgebraExpr(ConstantOp.fromValue(regex))).conditionPlan(wordNet, bindings, context))
  }
}

case class ContextReferenceReq(pos: Int) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = ContextRefOp(pos, bindings.lookupContextVariableType(pos))
}

case class BooleanByFilterReq(conditionalExpr: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    IfElseOp(SelectOp(ConstantOp.fromValue(true), conditionalExpr.conditionPlan(wordNet, bindings, context)),
      ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = variable match {
    case PathVariable(name) =>
      bindings.lookupPathVariableType(name).map(PathVariableRefOp(name, _))
        .getOrElse(throw new WQueryStaticCheckException("A reference to unknown variable @" + name + " found"))
    case StepVariable(name) =>
      bindings.lookupStepVariableType(name).map(StepVariableRefOp(name, _))
        .getOrElse(throw new WQueryStaticCheckException("A reference to unknown variable $" + name + " found"))
  }
}

case class ArcByArcExprReq(expr: ArcExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = {
    val pattern = if (context.creation) expr.creationPattern(wordNet) else expr.evaluationPattern(wordNet, DataType.all)

    pattern.relation.map { relation =>
      val arcs = pattern.destinations.map(destination => List(Arc(pattern.relation.get, pattern.source.name, destination.name)))
      ConstantOp(DataSet(if (arcs.isEmpty) List(List(Arc(pattern.relation.get, pattern.source.name, pattern.source.name))) else arcs))
    }.getOrElse {
      val toMap = pattern.destinations.map(arg => (arg.name, arg.nodeType)).toMap
      val arcs = (for (relation <- wordNet.relations;
           source <- relation.argumentNames if (pattern.source.name == "_" || pattern.source.name == source)  && pattern.source.nodeType.map(_ == relation.demandArgument(source).nodeType).getOrElse(true);
           destination <- relation.argumentNames if toMap.isEmpty || toMap.get(destination).map(nodeTypeOption =>
             nodeTypeOption.map(_ == relation.demandArgument(destination).nodeType).getOrElse(true)).getOrElse(false);
           if relation.arguments.size == 1 || source != destination)
        yield List(Arc(relation, source, destination)))

      ConstantOp(DataSet(arcs))
    }
  }
}

case class AlgebraExpr(op: AlgebraOp) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNetSchema, bindings: BindingsSchema, context: Context) = op
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
