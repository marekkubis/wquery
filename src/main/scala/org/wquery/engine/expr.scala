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

  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = EvaluateOp(this)
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

case class RelationalAliasExpr(name: String, relationalExpr: RelationalExpr) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.bindRelationalExprAlias(name, relationalExpr)
    DataSet.empty
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

    // TODO implement static type check

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
  }
}

case class MinusExpr(expr: EvaluableExpr) extends EvaluableExpr {
  // TODO provide static type check
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = MinusOp(expr.evaluationPlan(wordNet, bindings))
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
      ((avalues.maxPathSize - 1) to 0 by -1).map(avalues.getType(_)).toList.map {
          case UnionType(utypes) =>
            if (utypes == Set(FloatType, IntegerType)) ValueType(FloatType) else TupleType
          case t: BasicType =>
            ValueType(t)
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

sealed abstract class GeneratingTransformationExpr extends TransformationExpr {
  def generate(wordNet: WordNet, bindings: Bindings): DataSet
}

case class QuantifiedTransformationExpr(expr: PositionedRelationChainTransformationExpr, quantifier: Quantifier) extends GeneratingTransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    val lowerResult = (1 to quantifier.lowerBound).foldLeft(dataSet)((x, _) => expr.transform(wordNet, bindings, x))

    closureTransformation(wordNet, bindings, lowerResult, quantifier.upperBound.map(_ - quantifier.lowerBound))
  }

  def generate(wordNet: WordNet, bindings: Bindings) = {
    val dataSet = expr.generate(wordNet, bindings)
    val lowerResult = (2 to quantifier.lowerBound).foldLeft(dataSet)((x, _) => expr.transform(wordNet, bindings, x))

    closureTransformation(wordNet, bindings, lowerResult, quantifier.upperBound.map(_ - quantifier.lowerBound))
  }

  private def closureTransformation(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, limit: Option[Int]) = {
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
   //   val prefix = tuple.slice(0, tuple.size - 1)

      pathBuffer.append(tuple)
      pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
      stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))

      for (cnode <- closure(wordNet, bindings, tuple, Set(tuple.last), limit)) {
        pathBuffer.append(cnode)
        pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
        stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  private def closure(wordNet: WordNet, bindings: Bindings, source: List[Any], forbidden: Set[Any], limit: Option[Int]): List[List[Any]] = {
    if (limit.getOrElse(1) > 0) {
	    val transformed = expr.transform(wordNet, bindings, DataSet.fromTuple(source))
      val filtered = transformed.paths.filter { x => !forbidden.contains(x.last) }
	    val newLimit = limit.map(_ - 1).orElse(None)

      if (filtered.isEmpty) {
        filtered
      } else {
        val result = new ListBuffer[List[Any]]
        val newForbidden: Set[Any] = forbidden.++[Any, Set[Any]](filtered.map(_.last)) // TODO ugly

        result.appendAll(filtered)

        filtered.foreach { x =>
          result.appendAll(closure(wordNet, bindings, x, newForbidden, newLimit))
        }

        result.toList
      }
    } else {
      Nil
    }
  }
}

case class PositionedRelationTransformationExpr(pos: Int, rexpr: RelationalExpr) extends GeneratingTransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = rexpr.transform(wordNet, bindings, dataSet, pos)

  def generate(wordNet: WordNet, bindings: Bindings) = rexpr.generate(wordNet, bindings)
}

case class PositionedRelationChainTransformationExpr(exprs: List[PositionedRelationTransformationExpr]) extends GeneratingTransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    exprs.foldLeft(dataSet)((x, expr) => expr.transform(wordNet, bindings, x))
  }

  def generate(wordNet: WordNet, bindings: Bindings) = {
    exprs.tail.foldLeft(exprs.head.generate(wordNet, bindings))((x, expr) => expr.transform(wordNet, bindings, x))
  }
}

case class FilterTransformationExpr(cond: ConditionalExpr) extends GeneratingTransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      val binds = Bindings(bindings)

      for (pathVar <- pathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      for (stepVar <- stepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }

      binds.bindContextVariables(tuple)

      if (cond.satisfied(wordNet, binds)) {
        pathBuffer.append(tuple)
        pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
        stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }

  def generate(wordNet: WordNet, bindings: Bindings) = DataSet.fromValue(cond.satisfied(wordNet, bindings))
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys    
    val binds = Bindings(bindings)	    	
    
    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      
      for (pathVar <- pathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }
      
      for (stepVar <- stepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }
     
      buffer.append(expr.evaluationPlan(wordNet, bindings).evaluate(wordNet, binds))
    }
    
    buffer.toDataSet
  }  	
}

case class BindTransformationExpr(decls: List[Variable]) extends TransformationExpr with VariableBindings {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    bind(dataSet, decls)
  }
}

case class PathExpr(generator: EvaluableExpr, steps: List[TransformationExpr]) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    // separate projections
    steps.foldLeft(generator.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings))((step, trans) => trans.transform(wordNet, bindings, step))
  }
}

/*
 * Relational Expressions
 */
sealed abstract class RelationalExpr extends Expr {
  def generate(wordNet: WordNet, bindings: Bindings): DataSet
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet): Boolean
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int): DataSet
}

case class UnaryRelationalExpr(ids: List[String]) extends RelationalExpr {
  def generate(wordNet: WordNet, bindings: Bindings) = {	
    if (ids.size > 1) {
        useIdentifiersAsRelationTuplesGenerator(wordNet)
          .getOrElse(throw new WQueryEvaluationException("Relation '" + ids.head + "' not found"))
    } else {
      if (!bindings.areContextVariablesBound) {
        useIdentifiersAsRelationTuplesGenerator(wordNet)
          .getOrElse(useIdentifierAsBasicTypeGenerator(wordNet))
      } else {             
        useIdentifierAsBasicTypeGenerator(wordNet)	                
      }
    }				  
  }
  
  private def useIdentifiersAsRelationTuplesGenerator(wordNet: WordNet) = {
    ids match {
      case first::second::dests =>
        wordNet.findFirstRelationByNameAndSource(second, first)
          .map(r => wordNet.generateAllTuples(r, first::dests))
          .orElse(wordNet.findFirstRelationByNameAndSource(first, Relation.Source)
            .map(r => wordNet.generateAllTuples(r, Relation.Source::second::dests)))
      case List(head) =>
        wordNet.findFirstRelationByNameAndSource(head, Relation.Source) 
          .map(r => wordNet.generateAllTuples(r, Relation.Source::r.argumentNames.filter(_ != Relation.Source)))
      case Nil =>
          None        
    }
  }

  private def useIdentifierAsBasicTypeGenerator(wordNet: WordNet) = {
    ids.head match {
      case "false" =>
        DataSet.fromValue(false)
      case "true" =>
        DataSet.fromValue(true)
      case id =>
        wordNet.getWordForm(id)
    }
  }  
  
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    (ids.size > 1) || wordNet.containsRelation(ids.head, dataSet.getType(0), Relation.Source)
  }
	
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {	  
    if (ids.size > 1) {
      useIdentifiersAsTransformation(wordNet, dataSet, pos)  
    } else {
      useIdentifierAsRelationalExprAlias(wordNet, bindings, dataSet, pos)
        .getOrElse(useIdentifiersAsTransformation(wordNet, dataSet, pos))           
    }
  }

  private def useIdentifiersAsTransformation(wordNet: WordNet, dataSet: DataSet, pos: Int) = {
    if (dataSet.isEmpty) {
      dataSet
    } else {
      if (ids == List("_")) {
        wordNet.followAny(dataSet, pos)
      } else {
        val sourceType = dataSet.getType(pos - 1)
        val (relation, source, dests) = (ids: @unchecked) match {
          case List(relationName) =>
            (wordNet.demandRelation(relationName, sourceType, Relation.Source), Relation.Source, List(Relation.Destination))
          case List(left, right) =>
            wordNet.getRelation(left, sourceType, Relation.Source)
              .map((_, Relation.Source, List(right)))
              .getOrElse((wordNet.demandRelation(right, sourceType, left), left, List(Relation.Destination)))
          case first :: second :: dests =>
            wordNet.getRelation(first, sourceType, Relation.Source)
              .map((_, Relation.Source, second :: dests))
              .getOrElse((wordNet.demandRelation(second, sourceType, first), first, dests))
        }

        wordNet.followRelation(dataSet, pos, relation, source, dests)
      }
    }
  }
  
  private def useIdentifierAsRelationalExprAlias(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    bindings.lookupRelationalExprAlias(ids.head).map(_.transform(wordNet, bindings, dataSet, pos))      
  }
}

case class UnionRelationalExpr(exprs: List[RelationalExpr]) extends RelationalExpr {
  def generate(wordNet: WordNet, bindings: Bindings) = {
    DataSet(exprs.flatMap(_.generate(wordNet, bindings).paths))
  }
  
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
	  exprs.forall(_.canTransform(wordNet, bindings, dataSet))
  }
	
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    val results = exprs.map(_.transform(wordNet, bindings, dataSet, pos))

    DataSet(
      results.flatMap(_.paths),
      results.head.pathVars.map(pathVar => (pathVar._1, results.flatMap(_.pathVars(pathVar._1)))),
      results.head.stepVars.map(stepVar => (stepVar._1, results.flatMap(_.stepVars(stepVar._1))))
    )
  }
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
          if (BasicType(rresult.head) == StringType) {
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
case class SynsetByExprReq(expr: EvaluableExpr) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluationPlan(wordNet, bindings).evaluate(wordNet, bindings)

    if (eresult.minPathSize == 1 && eresult.maxPathSize == 1) {
      if (eresult.getType(0) == SenseType) {
        wordNet.getSynsetsBySenses(eresult.paths.map(_.head.asInstanceOf[Sense]))
      } else if (eresult.getType(0) == StringType) {
        wordNet.getSynsetsByWordForms(eresult.paths.map(_.head.asInstanceOf[String]))
      } else {
        throw new WQueryEvaluationException("{...} requires an expression that generates either senses or word forms")  
      }           
    } else if (eresult.pathCount == 0) {
      DataSet.empty
    } else {
      throw new WQueryEvaluationException("{...} requires an expression that generates single element paths")        
    }
  }
}

case class ContextByRelationalExprReq(expr: RelationalExpr, quantifier: Quantifier) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val qte = QuantifiedTransformationExpr(PositionedRelationChainTransformationExpr(List(PositionedRelationTransformationExpr(1, expr))), quantifier)

	  if (bindings.areContextVariablesBound) {
	    val dataSet = DataSet(List(bindings.contextVariables))

	    if (expr.canTransform(wordNet, bindings, dataSet))
        qte.transform(wordNet, bindings, dataSet)
  	  else
        qte.generate(wordNet, bindings)
    } else {
      qte.generate(wordNet, bindings)
    }
  }  
}

case class WordFormByRegexReq(regex: String) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings)
    = SelectOp(FetchOp.words, ComparisonExpr("=~", AlgebraExpr(ContextRefOp(1)), AlgebraExpr(ConstantOp.fromValue(regex))))
}

case class BooleanByFilterReq(condition: ConditionalExpr) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = IfElseOp(SelectOp(ConstantOp.fromValue(true), condition),
    ConstantOp.fromValue(true), Some(ConstantOp.fromValue(false)))
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluationPlan(wordNet: WordNet, bindings: Bindings) = variable match {
    case PathVariable(name) =>
      if (bindings.isPathVariableBound(name))
        PathVariableRefOp(name)
      else
        throw new WQueryEvaluationException("A reference to unknown variable @'" + name + "' found")
    case StepVariable(name) =>
      if (bindings.isStepVariableBound(name))
        StepVariableRefOp(name)
      else
        throw new WQueryEvaluationException("A reference to unknown variable $'" + name + "' found")
  }
}

case class ArcByUnaryRelationalExprReq(rexpr: UnaryRelationalExpr) extends SelfPlannedExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val ids = rexpr match {case UnaryRelationalExpr(x) => x}

    ids match {
      case first::second::dests =>
        wordNet.findFirstRelationByNameAndSource(second, first)
          .map { r => 
            r.demandArgument(first)
            
            if (dests.isEmpty) {
              DataSet.fromValue(Arc(r, first, Relation.Destination))	
            } else {
              DataSet.fromList(dests.map { dest => 
                r.demandArgument(dest)
                Arc(r, first, dest)
              })            	
            }
          }
          .orElse(wordNet.findFirstRelationByNameAndSource(first, Relation.Source)
            .map { r => 
              DataSet.fromList((second::dests).map { dest =>
                r.demandArgument(dest)
                Arc(r, Relation.Source, dest)		
            })})
            .getOrElse(throw new WQueryEvaluationException("Arc generator references an unknown relation"))
      case List(head) =>
        wordNet.findFirstRelationByNameAndSource(head, Relation.Source) 
          .map(r => DataSet.fromValue(Arc(r, Relation.Source, Relation.Destination )))
          .getOrElse(throw new WQueryEvaluationException("Arc generator references unknown relation '" + head + "'"))
      case Nil =>
        throw new RuntimeException("ids is empty in ArcByUnaryRelationalExprReq")
    }    
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
case class Quantifier(val lowerBound: Int, val upperBound: Option[Int]) extends Expr