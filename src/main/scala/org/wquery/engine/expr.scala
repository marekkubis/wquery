package org.wquery.engine
import org.wquery.{WQueryEvaluationException, WQueryModelException}
import org.wquery.model.{WordNet, IntegerType, Relation, DataType, StringType, SenseType, Sense, BasicType, UnionType, ValueType, TupleType, AggregateFunction, ScalarFunction, FloatType, FunctionArgumentType, Arc, DataSet, DataSetBuffer, DataSetBuffers}
import scala.collection.mutable.ListBuffer

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet
}

sealed abstract class BindingsFreeExpr extends EvaluableExpr {
  def evaluate(wordNet: WordNet): DataSet

  def evaluate(wordNet: WordNet, bindings: Bindings): DataSet = evaluate(wordNet)
}

sealed abstract class SelfEvaluableExpr extends BindingsFreeExpr {
  def evaluate(): DataSet

  def evaluate(wordNet: WordNet): DataSet = evaluate
}

/*
 * Imperative expressions
 */

sealed abstract class ImperativeExpr extends EvaluableExpr

case class EmissionExpr(expr: EvaluableExpr) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    expr.evaluate(wordNet, bindings)
  }
}

case class IteratorExpr(pexpr: EvaluableExpr, iexpr: ImperativeExpr) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val presult = pexpr.evaluate(wordNet, bindings)
    val tupleBindings = Bindings(bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = presult.pathVars.keys.toSeq
    val stepVarNames = presult.stepVars.keys.toSeq

    for (i <- 0 until presult.pathCount) {
      val tuple = presult.paths(i)
        
      pathVarNames.foreach { pathVar =>
        val varPos = presult.pathVars(pathVar)(i)
        tupleBindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      stepVarNames.foreach(stepVar => tupleBindings.bindStepVariable(stepVar, tuple(presult.stepVars(stepVar)(i))))
      buffer.append(iexpr.evaluate(wordNet, tupleBindings))
    }

    buffer.toDataSet
  }
}

case class IfElseExpr(cexpr: EvaluableExpr, iexpr: ImperativeExpr, eexpr: Option[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    if (cexpr.evaluate(wordNet, bindings).isTrue)
      iexpr.evaluate(wordNet, bindings)
    else
      eexpr.map(_.evaluate(wordNet, bindings)).getOrElse(DataSet.empty)        
  }
}

case class BlockExpr(exprs: List[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val blockBindings = Bindings(bindings)
    val buffer = new DataSetBuffer

    for (expr <- exprs) {
      buffer.append(expr.evaluate(wordNet, blockBindings))
    }

    buffer.toDataSet
  }
}

case class EvaluableAssignmentExpr(decls: List[Variable], expr: EvaluableExpr) extends ImperativeExpr with VariableBindings {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(wordNet, bindings)

    if (eresult.pathCount == 1) { // TODO remove this constraint
      val tuple = eresult.paths.head
      val dataSet = bind(DataSet.fromTuple(tuple), decls)
      
      dataSet.pathVars.keys.foreach { pathVar => 
        val varPos = dataSet.pathVars(pathVar)(0)
        bindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }
      
      dataSet.stepVars.keys.foreach(stepVar => bindings.bindStepVariable(stepVar, tuple(dataSet.stepVars(stepVar)(0))))
      DataSet.empty
    } else {
      throw new WQueryEvaluationException("A multipath expression in an assignment should contain exactly one tuple")
    }
  }
}

case class RelationalAssignmentExpr(name: String, rexpr: RelationalExpr) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.bindRelationalExprAlias(name, rexpr)
    DataSet.empty
  }
}

case class WhileDoExpr(cexpr: EvaluableExpr, iexpr: ImperativeExpr) extends ImperativeExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val buffer = new DataSetBuffer      
      
    while (cexpr.evaluate(wordNet, bindings).isTrue)
      buffer.append(iexpr.evaluate(wordNet, bindings))
      
    buffer.toDataSet  
  }
}
/*
 * Multipath expressions
 */
case class BinaryPathExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val leval = left.evaluate(wordNet, bindings)
    val reval = right.evaluate(wordNet, bindings)

    op match {
      case "," =>
        join(leval, reval)
      case op =>
        DataSet(
          op match {
            case "union" =>
              leval.paths union reval.paths
            case "except" =>
              leval.paths.filterNot(reval.paths.contains)
            case "intersect" =>
              leval.paths intersect reval.paths
            case _ =>
              throw new IllegalArgumentException("Unknown binary path operator '" + op + "'")
          })
    }
  }

  def join(left: DataSet, right: DataSet) = {
    val leftPathVarNames = left.pathVars.keys.toSeq
    val leftStepVarNames = left.stepVars.keys.toSeq
    val rightPathVarNames = right.pathVars.keys.toSeq
    val rightStepVarNames = right.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(leftPathVarNames ++ rightPathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(leftStepVarNames ++ rightStepVarNames)
    
    for (i <- 0 until left.pathCount) {
      for (j <- 0 until right.pathCount ) {
        val leftTuple = left.paths(i)
        val rightTuple = right.paths(j)        
          
        pathBuffer.append(leftTuple ++ rightTuple)
        leftPathVarNames.foreach(x => pathVarBuffers(x).append(left.pathVars(x)(i)))
        leftStepVarNames.foreach(x => stepVarBuffers(x).append(left.stepVars(x)(i)))
        
        val offset = leftTuple.size
        
        rightPathVarNames.foreach { x => 
          val pos = right.pathVars(x)(j)          
          pathVarBuffers(x).append((pos._1 + offset, pos._2 + offset))
        }
        
        rightStepVarNames.foreach(x => stepVarBuffers(x).append(right.stepVars(x)(j) + offset))
      }
    }

    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
  }
}

/*
 * Arithmetic expressions
 */
case class BinaryArithmExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val lresult = left.evaluate(wordNet, bindings)
    val rresult = right.evaluate(wordNet, bindings)

    if (lresult.minPathSize > 0 && rresult.minPathSize > 0 && lresult.isNumeric(0) && rresult.isNumeric(0)) {
      if (lresult.getType(0) == IntegerType && rresult.getType(0) == IntegerType) {
        combineUsingIntArithmOp(op, lresult.paths.map( _.last.asInstanceOf[Int]), rresult.paths.map( _.last.asInstanceOf[Int]))
      } else {
        combineUsingDoubleArithmOp(op, 
          lresult.paths.map(_.last).map {
            case x: Double => x            
            case x: Int => x.doubleValue
          },
          rresult.paths.map(_.last).map {
            case x: Double => x              
            case x: Int => x.doubleValue
          }
        )
      }
    } else {
      throw new WQueryEvaluationException("Operator '" + op + "' requires paths that end with float or integer values") 
    }
  }

  def combineUsingIntArithmOp(op: String, leval: List[Int], reval: List[Int]) = {
    val func: (Int, Int) => Int = op match {
      case "+" =>
        (x, y) => x + y
      case "-" =>
        (x, y) => x - y
      case "*" =>
        (x, y) => x * y
      case "/" =>
        (x, y) => x / y
      case "%" =>
        (x, y) => x % y
      case _ =>
        throw new IllegalArgumentException("Unknown binary arithmetic operator '" + op + "'")
    }
    
    combine[Int](func, leval, reval)
  }    
  
  def combineUsingDoubleArithmOp(op: String, leval: List[Double], reval: List[Double]) = {
    val func: (Double, Double) => Double = op match {
      case "+" =>
        (x, y) => x + y
      case "-" =>
        (x, y) => x - y
      case "*" =>
        (x, y) => x * y
      case "/" =>
        (x, y) => x / y
      case "%" =>
        (x, y) => x % y
      case _ =>
        throw new IllegalArgumentException("Unknown binary arithmetic operator '" + op + "'")
    }
    
    combine[Double](func, leval, reval)
  }  

  def combine[A](func: (A, A) => A, leval: List[A], reval: List[A]) = {
    val buffer = new ListBuffer[List[Any]]

    for (lval <- leval) {
      for (rval <- reval) {
        buffer append List(func(lval, rval))
      }
    }

    DataSet(buffer.toList)
  }

}

case class MinusExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(wordNet, bindings)

    if (eresult.isNumeric(0)) {
        DataSet(eresult.paths.map(_.last).map {
            case x: Int => List(-x)
            case x: Double => List(-x)
        })
    } else {
      throw new WQueryEvaluationException("Unary '-' requires a context that consist of integer or float singletons")       
    }
  }
}

/* 
 * Function call expressions
 */
case class FunctionExpr(name: String, args: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val avalues = args.evaluate(wordNet, bindings)
    
    val atypes: List[FunctionArgumentType] = if (avalues.minPathSize  != avalues.maxPathSize ) {
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
        var stop = false

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
 * Path related expressions
 */
case class StepExpr(lexpr: EvaluableExpr, rexpr: TransformationExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = rexpr.transform(wordNet, bindings, lexpr.evaluate(wordNet, bindings))
}

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

case class RelationTransformationExpr(pos: Int, rexpr: RelationalExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {	
	rexpr.transform(wordNet, bindings, dataSet, pos)
  }
}

case class FilterTransformationExpr(cond: ConditionalExpr) extends TransformationExpr {
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
}

case class ProjectionTransformationExpr(expr: EvaluableExpr) extends TransformationExpr {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    val buffer = new DataSetBuffer
    val pathVarNames = dataSet.pathVars.keys
    val stepVarNames = dataSet.stepVars.keys    
    val binds = Bindings(bindings)	    	
    
    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)
      val tupleBuffer = new ListBuffer[Any]
      
      for (pathVar <- pathVarNames) {
        val varPos = dataSet.pathVars(pathVar)(i)
        binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }
      
      for (stepVar <- stepVarNames) {
        val varPos = dataSet.stepVars(stepVar)(i)
        binds.bindStepVariable(stepVar, tuple(varPos))
      }
     
      buffer.append(expr.evaluate(wordNet, binds))
    }
    
    buffer.toDataSet
  }  	
}

case class BindTransformationExpr(decls: List[Variable]) extends TransformationExpr with VariableBindings {
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = bind(dataSet, decls)	
}

case class PathExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = { //TODO extend this method or remove this class
    expr.evaluate(wordNet, bindings)
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
        useIdentifiersAsRelationPathsGenerator(wordNet)
          .getOrElse(throw new WQueryEvaluationException("Relation '" + ids.head + "' not found"))
    } else {
      if (!bindings.areContextVariablesBound) {
        useIdentifiersAsRelationPathsGenerator(wordNet)
          .getOrElse(useIdentifierAsBasicTypeGenerator(wordNet))
      } else {             
        useIdentifierAsBasicTypeGenerator(wordNet)	                
      }
    }				  
  }
  
  private def useIdentifiersAsRelationPathsGenerator(wordNet: WordNet) = {
    ids match {
      case first::second::dests =>
        wordNet.findFirstRelationByNameAndSource(second, first)
          .map(r => DataSet(wordNet.getPaths(r, first, dests)))
          .orElse(wordNet.findFirstRelationByNameAndSource(first, Relation.Source)
            .map(r => DataSet(wordNet.getPaths(r, Relation.Source, second::dests))))
      case List(head) =>
        wordNet.findFirstRelationByNameAndSource(head, Relation.Source) 
          .map(r => DataSet(wordNet.getPaths(r, Relation.Source, r.argumentNames.filter(_ != Relation.Source))))        
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
        DataSet.fromOptionalValue(wordNet.getWordForm(id))                    
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
	if (ids == List("_")) {
	  wordNet.followAny(dataSet, pos)	  
	} else {
      val sourceType = dataSet.getType(pos - 1)  
      val (relation, source, dests) = ids match {
        case List(relationName) =>
          (wordNet.demandRelation(relationName, sourceType, Relation.Source), Relation.Source, List(Relation.Destination))
        case List(left, right) =>
          wordNet.getRelation(left, sourceType, Relation.Source) 
            .map((_, Relation.Source, List(right)))
            .getOrElse((wordNet.demandRelation(right, sourceType, left), left, List(Relation.Destination)))
        case first::second::dests =>
          wordNet.getRelation(first, sourceType, Relation.Source) 
            .map((_, Relation.Source, second::dests))
            .getOrElse((wordNet.demandRelation(second, sourceType, first), first, dests))
      }
    
      wordNet.followRelation(dataSet, pos, relation, source, dests)		
	}
  }
  
  private def useIdentifierAsRelationalExprAlias(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    bindings.lookupRelationalExprAlias(ids.head).map(_.transform(wordNet, bindings, dataSet, pos))      
  }
}

case class QuantifiedRelationalExpr(expr: RelationalExpr, quantifier: Quantifier) extends RelationalExpr {
  def generate(wordNet: WordNet, bindings: Bindings) = {
    quantifier match {
	  case Quantifier(1, Some(1)) =>
	    expr.generate(wordNet, bindings)
	  case Quantifier(1, None) =>
	    closureTransformation(wordNet, bindings, expr.generate(wordNet, bindings))
	}
  }
  
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = expr.canTransform(wordNet, bindings, dataSet)
  
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    quantifier match {
      case Quantifier(1, Some(1)) =>
        expr.transform(wordNet, bindings, dataSet, pos)
      case Quantifier(1, None) =>
	    closureTransformation(wordNet, bindings, expr.transform(wordNet, bindings, dataSet, pos))
      case _ =>
        throw new IllegalArgumentException("Unsupported quantifier value " + quantifier)
    }
  }
  
  private def closureTransformation(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
    val pathVarNames = dataSet.pathVars.keys.toSeq
    val stepVarNames = dataSet.stepVars.keys.toSeq
    val pathBuffer = DataSetBuffers.createPathBuffer
    val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
    val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)        

    for (i <- 0 until dataSet.pathCount) {
      val tuple = dataSet.paths(i)        	
      val prefix = tuple.slice(0, tuple.size - 1)
      
      pathBuffer.append(tuple)
      pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
      stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))          

      for (cnode <- closure(wordNet, bindings, expr, List(tuple.last), Set(tuple.last))) {
        pathBuffer.append(prefix ++ cnode)
        pathVarNames.foreach(x => pathVarBuffers(x).append(dataSet.pathVars(x)(i)))
        stepVarNames.foreach(x => stepVarBuffers(x).append(dataSet.stepVars(x)(i)))        
      }
    }
    
    DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)	  
  }
  
  private def closure(wordNet: WordNet, bindings: Bindings, expr: RelationalExpr, source: List[Any], forbidden: Set[Any]): List[List[Any]] = {
    val transformed = expr.transform(wordNet, bindings, DataSet.fromTuple(source), 1)
    val filtered = transformed.paths.filter { x => !forbidden.contains(x.last) }

    if (filtered.isEmpty) {
      filtered
    } else {
      val result = new ListBuffer[List[Any]]
      val newForbidden: Set[Any] = forbidden.++[Any, Set[Any]](filtered.map(_.last)) // TODO ugly

      result.appendAll(filtered)

      filtered.foreach { x =>
        result.appendAll(closure(wordNet, bindings, expr, x, newForbidden))
      }

      result.toList
    }
  }  
}

case class ComposeRelationalExpr(lexpr: RelationalExpr, rexpr: RelationalExpr) extends RelationalExpr {
  def generate(wordNet: WordNet, bindings: Bindings) = {
    rexpr.transform(wordNet, bindings, lexpr.generate(wordNet, bindings), 1)
  }
  
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
	lexpr.canTransform(wordNet, bindings, dataSet) && rexpr.canTransform(wordNet, bindings, lexpr.transform(wordNet, bindings, dataSet, 1))
  }
	
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    rexpr.transform(wordNet, bindings, lexpr.transform(wordNet, bindings, dataSet, pos), 1)
  }
}

case class UnionRelationalExpr(lexpr: RelationalExpr, rexpr: RelationalExpr) extends RelationalExpr {
  def generate(wordNet: WordNet, bindings: Bindings) = {
    val leval = lexpr.generate(wordNet, bindings)
    val reval = rexpr.generate(wordNet, bindings)

    DataSet(leval.paths  union reval.paths) 	  
  }
  
  def canTransform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet) = {
	lexpr.canTransform(wordNet, bindings, dataSet) && rexpr.canTransform(wordNet, bindings, dataSet) 	  
  }
	
  def transform(wordNet: WordNet, bindings: Bindings, dataSet: DataSet, pos: Int) = {
    val lresult = lexpr.transform(wordNet, bindings, dataSet, pos)
    val rresult = rexpr.transform(wordNet, bindings, dataSet, pos)

    DataSet(lresult.paths union rresult.paths,
      lresult.pathVars.map(x => (x._1, x._2 ++ rresult.pathVars(x._1))),
      lresult.stepVars.map(x => (x._1, x._2 ++ rresult.stepVars(x._1)))
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
    val lresult = lexpr.evaluate(wordNet, bindings).paths.map(_.last)
    val rresult = rexpr.evaluate(wordNet, bindings).paths.map(_.last)

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
            throw new WQueryEvaluationException("The right side of '" + op +
              "' should return exactly one character string value")
          }
        } else if (rresult.isEmpty) {
          throw new WQueryEvaluationException("The right side of '" + op + "' returns no values")
        } else { // rresult.pathCount > 0
          throw new WQueryEvaluationException("The right side of '" + op + "' returns more than one values")
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
            throw new WQueryEvaluationException("The left side of '" + op + "' returns no values")
          if (lresult.size > 1)
            throw new WQueryEvaluationException("The left side of '" + op + "' returns more than one value")
          if (rresult.isEmpty)
            throw new WQueryEvaluationException("The right side of '" + op + "' returns no values")
          if (rresult.size > 1)
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
case class BooleanByValueReq(value: Boolean) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class SynsetAllReq() extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.synsets.toList)
}

case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(wordNet, bindings)

    if (eresult.pathCount == 1 && eresult.minPathSize == 1 && eresult.maxPathSize == 1) {
      if (eresult.getType(0) == SenseType) {
        DataSet(eresult.paths.map { case List(sense: Sense) => List(wordNet.demandSynsetBySense(sense)) })
      } else if (eresult.getType(0) == StringType) {          
        DataSet(eresult.paths.flatMap { case List(wordForm: String) => wordNet.getSynsetsByWordForm(wordForm) }.map(List(_)))  
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

case class SenseAllReq() extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.senses.toList)
}

case class SenseByWordFormAndSenseNumberAndPosReq(word: String, num: Int, pos: String) extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromOptionalValue(wordNet.getSenseByWordFormAndSenseNumberAndPos(word, num, pos))
}

case class SenseByWordFormAndSenseNumberReq(word: String, num: Int) extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.getSensesByWordFormAndSenseNumber(word, num))
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {			  
	if (bindings.areContextVariablesBound) {
	  val dataSet = DataSet(List(bindings.contextVariables))

	  if (expr.canTransform(wordNet, bindings, dataSet))
	    expr.transform(wordNet, bindings, dataSet, 1)
	  else
        expr.generate(wordNet, bindings)
    } else {
      expr.generate(wordNet, bindings)
    }		
  }  
}

case class StringByValueReq(value: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class WordFormByRegexReq(v: String) extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = {
    val regex = v.r
    val result = wordNet.words.filter(x => regex.findFirstIn(x).map(_ => true).getOrElse(false))

    DataSet.fromList(result.toList)
  }
}

case class WordFormByValueReq(value: String) extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet) = {
    if (value == "")
      DataSet.fromList(wordNet.words.toList)
    else
      DataSet.fromOptionalValue(wordNet.getWordForm(value))
  }
}

case class FloatByValueReq(value: Double) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class SequenceByBoundsReq(left: Int, right: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromList((left to right).toList)
}

case class IntegerByValueReq(value: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class ContextByReferenceReq(ref: Int) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable(ref).map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Backward reference '" + List.fill(ref)("#").mkString + "' too far"))
  }
}

case class ContextByVariableReq(variable: Variable) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = {
    variable match {
      case PathVariable(name) =>
        bindings.lookupPathVariable(name).map(DataSet.fromTuple(_))
          .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable @'" + name + "' found"))
      case StepVariable(name) =>
        bindings.lookupStepVariable(name).map(DataSet.fromValue(_))
          .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable $'" + name + "' found"))
    }
  }
}

case class ArcByUnaryRelationalExprReq(rexpr: UnaryRelationalExpr) extends EvaluableExpr {
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

case class BooleanByFilterReq(cond: ConditionalExpr) extends EvaluableExpr {
  def evaluate(wordNet: WordNet, bindings: Bindings) = DataSet.fromValue(cond.satisfied(wordNet, bindings))
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
case class Quantifier(left: Int, right: Option[Int]) extends Expr