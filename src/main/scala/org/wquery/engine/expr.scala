package org.wquery.engine
import org.wquery.WQueryEvaluationException
import org.wquery.model.{WordNet, IntegerType, Relation, DataType, StringType, SenseType, SynsetType, Sense, BasicType, UnionType, ValueType, TupleType, AggregateFunction, ScalarFunction, FloatType, FunctionArgumentType, Arc, DataSet, DataSetBuffer, DataSetBuffers}
import scala.collection.mutable.ListBuffer

sealed abstract class Expr

sealed abstract class EvaluableExpr extends Expr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings): DataSet
}

sealed abstract class BindingsFreeExpr extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet): DataSet

  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings): DataSet = evaluate(functions, wordNet)
}

sealed abstract class FunctionsFreeExpr extends BindingsFreeExpr {
  def evaluate(wordNet: WordNet): DataSet

  def evaluate(functions: FunctionSet, wordNet: WordNet): DataSet = evaluate(wordNet)
}

sealed abstract class SelfEvaluableExpr extends FunctionsFreeExpr {
  def evaluate(): DataSet

  def evaluate(wordNet: WordNet): DataSet = evaluate
}

/*
 * Imperative expressions
 */

sealed abstract class ImperativeExpr extends EvaluableExpr

case class EmissionExpr(expr: EvaluableExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    expr.evaluate(functions, wordNet, bindings)
  }
}

case class IteratorExpr(pexpr: EvaluableExpr, iexpr: ImperativeExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val presult = pexpr.evaluate(functions, wordNet, bindings)
    val tupleBindings = Bindings(bindings)
    val buffer = new DataSetBuffer
    val pathVarNames = presult.pathVars.keys.toSeq
    val stepVarNames = presult.stepVars.keys.toSeq

    for (i <- 0 until presult.pathCount) {
      val tuple = presult.paths(i)
        
      pathVarNames.foreach { pathVar =>
        val varPos = presult.pathVars(pathVar)(i)
        tupleBindings.bindStepVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }

      stepVarNames.foreach(stepVar => tupleBindings.bindStepVariable(stepVar, tuple(presult.stepVars(stepVar)(i))))
      buffer.append(iexpr.evaluate(functions, wordNet, tupleBindings))
    }

    buffer.toDataSet
  }
}

case class IfElseExpr(cexpr: EvaluableExpr, iexpr: ImperativeExpr, eexpr: Option[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    if (cexpr.evaluate(functions, wordNet, bindings).isTrue)
      iexpr.evaluate(functions, wordNet, bindings)
    else
      eexpr.map(_.evaluate(functions, wordNet, bindings)).getOrElse(DataSet.empty)        
  }
}

case class BlockExpr(exprs: List[ImperativeExpr]) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val blockBindings = Bindings(bindings)
    val buffer = new DataSetBuffer

    for (expr <- exprs) {
      buffer.append(expr.evaluate(functions, wordNet, blockBindings))
    }

    buffer.toDataSet
  }
}

case class EvaluableAssignmentExpr(decls: List[VariableLit], expr: EvaluableExpr) extends ImperativeExpr with VariableBindings {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(functions, wordNet, bindings)

    if (eresult.pathCount == 1) { // TODO remove this constraint
      val tuple = eresult.paths.head
      val dataSet = bind(DataSet.fromTuple(tuple), decls)
      
      dataSet.pathVars.keys.foreach { pathVar => 
        val varPos = dataSet.pathVars(pathVar)(0)
        bindings.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))
      }
      
      dataSet.stepVars.keys.foreach(stepVar => bindings.bindStepVariable(stepVar , tuple(dataSet.stepVars(stepVar)(0))))
      DataSet.empty
    } else {
      throw new WQueryEvaluationException("A multipath expression in an assignment should contain exactly one tuple")
    }
  }
}

case class RelationalAssignmentExpr(name: String, rexpr: RelationalExpr) extends ImperativeExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    bindings.bindRelationalExprAlias(name, rexpr)
    DataSet.empty
  }
}

/*
 * Multipath expressions
 */
case class BinaryPathExpr(op: String, left: EvaluableExpr, right: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val leval = left.evaluate(functions, wordNet, bindings)
    val reval = right.evaluate(functions, wordNet, bindings)

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
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val lresult = left.evaluate(functions, wordNet, bindings)
    val rresult = right.evaluate(functions, wordNet, bindings)

    if (lresult.minPathSize > 0 && rresult.minPathSize > 0 && lresult.isNumeric(0) && rresult.isNumeric(0)) {
      if (lresult.getType(0) == IntegerType && rresult.getType(0) == IntegerType) {
        combineUsingIntArithmOp(op, lresult.paths.map( x => x.last.asInstanceOf[Int]), rresult.paths.map( x => x.last.asInstanceOf[Int]))
      } else {
        combineUsingDoubleArithmOp(op, 
          lresult.paths.map(x => x.last).map {
            case x: Double => x            
            case x: Int => x.doubleValue
          },
          rresult.paths.map(x => x.last).map {
            case x: Double => x              
            case x: Int => x.doubleValue
          }
        )
      }
    } else {
      throw new WQueryEvaluationException("Operator '" + op + "' requires numeric type ended datapaths") 
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
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(functions, wordNet, bindings)

    if (eresult.isNumeric(0)) {
        DataSet(eresult.paths.map(x => x.last).map {
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
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val avalues = args.evaluate(functions, wordNet, bindings)
    
    val atypes: List[FunctionArgumentType] = if (avalues.minPathSize  != avalues.maxPathSize ) {
      List(TupleType)
    } else {
      ((avalues.maxPathSize - 1) to 0 by -1).map(x => avalues.getType(x)).toList.map {
          case UnionType(utypes) =>
            if (utypes == Set(FloatType, IntegerType)) ValueType(FloatType) else TupleType
          case t: BasicType =>
            ValueType(t)
      }
    }

    functions.getFunction(name, atypes) 
      .map(invokeFunction(_, atypes, avalues))
      .getOrElse(functions.getFunction(
        name,
        atypes.map {
          case ValueType(IntegerType) => ValueType(FloatType)
          case t => t
        }).map(invokeFunction(_, atypes, avalues))
          .getOrElse(invokeFunction(functions.demandFunction(name, List(TupleType)), atypes, avalues))
      )
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
case class StepExpr(lexpr: EvaluableExpr, rexpr: TransformationDesc) extends EvaluableExpr with VariableBindings {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    rexpr match {
      case RelationTransformationDesc(pos, rexpr, decls) =>
        val lresult = lexpr.evaluate(functions, wordNet, bindings)

        bind(rexpr.transform(wordNet, bindings, lresult, pos, false), decls)
      case FilterTransformationDesc(cond, decls) =>
        val lresult = lexpr.evaluate(functions, wordNet, bindings)
        val pathVarNames = lresult.pathVars.keys.toSeq
        val stepVarNames = lresult.stepVars.keys.toSeq                
        val pathBuffer = DataSetBuffers.createPathBuffer
        val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
        val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)
        
        for (i <- 0 until lresult.pathCount) {
          val tuple = lresult.paths(i)
          val binds = Bindings(bindings)        
          
          for (pathVar <- pathVarNames) {
            val varPos = lresult.pathVars(pathVar)(i)
            binds.bindPathVariable(pathVar, tuple.slice(varPos._1, varPos._2))            
          }
          
          for (stepVar <- stepVarNames) {
            val varPos = lresult.stepVars(stepVar)(i)
            binds.bindStepVariable(stepVar, tuple(varPos))            
          }          
          
          binds.bindContextVariables(tuple)
          
          if (cond.satisfied(functions, wordNet, binds)) {
            pathBuffer.append(tuple)
            pathVarNames.foreach(x => pathVarBuffers(x).append(lresult.pathVars(x)(i)))
            stepVarNames.foreach(x => stepVarBuffers(x).append(lresult.stepVars(x)(i)))
          }
        }

        bind(DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers), decls)
    }
  }
}

trait VariableBindings {
  def bind(dataSet: DataSet, decls: List[VariableLit]) = {
    if (decls != Nil) {          
      val pathVarBuffers = DataSetBuffers.createPathVarBuffers(decls.filter(x => (x.isInstanceOf[PathVariableLit] && x.value != "_")).map(_.value))                
      val stepVarBuffers = DataSetBuffers.createStepVarBuffers(decls.filterNot(x => (x.isInstanceOf[PathVariableLit] || x.value == "_")).map(_.value))        
    
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
            dataSet.paths.foreach(tuple => bindVariablesFromLeft(leftVars, stepVarBuffers))            
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
  
  private def getPathVariablePosition(decls: List[VariableLit]) = {
    val pathVarPos = decls.indexWhere{_.isInstanceOf[PathVariableLit]}
    
    if (pathVarPos != decls.lastIndexWhere{_.isInstanceOf[PathVariableLit]}) {
      throw new WQueryEvaluationException("Variable list '" + decls.map { 
        case PathVariableLit(v) => "@" + v 
        case StepVariableLit(v) => "$" + v
      }.mkString + "' contains more than one path variable")
    } else {
      if (pathVarPos != -1)
        Some(pathVarPos)
      else
        None
    }
  }
  
  private def bindVariablesFromLeft(vars: Map[String, Int], varIndexes: Map[String, ListBuffer[Int]]) {
    for ((v, pos) <- vars)
      varIndexes(v).append(pos)
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

sealed abstract class TransformationDesc extends Expr

case class RelationTransformationDesc(pos: Int, expr: RelationalExpr, decls: List[VariableLit]) extends TransformationDesc
case class FilterTransformationDesc(expr: ConditionalExpr, decls: List[VariableLit]) extends TransformationDesc

case class PathExpr(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = { //TODO extend this method or remove this class
    expr.evaluate(functions, wordNet, bindings)
  }
}

/*
 * Relational Expressions
 */
sealed abstract class RelationalExpr extends Expr {
  def transform(wordNet: WordNet, bindings: Bindings, data: DataSet, pos: Int, force: Boolean): DataSet
}

case class UnaryRelationalExpr(idLits: List[IdentifierLit]) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, data: DataSet, pos: Int, force: Boolean) = {
    if (force || idLits.size > 1) {
      if (pos == 0) {
        if (!bindings.areContextVariablesBound) {// we are not in a filter
          useIdentifiersAsGenerator(idLits.map(_.value), wordNet)
            .getOrElse(throw new WQueryEvaluationException("Relation '" + idLits.head.value + "' not found"))
        } else {
          useIdentifiersAsTransformation(idLits.map(_.value), wordNet, data, 1)
        }
      } else {
        useIdentifiersAsTransformation(idLits.map(_.value), wordNet, data, pos)  
      }
    } else {
      idLits.head match {
        case QuotedIdentifierLit(wordForm) =>
          if (pos == 0)
            useIdentifierAsGenerator(wordForm, wordNet)
          else
            throw new WQueryEvaluationException("Quoted identifier found after '.'")
        case NotQuotedIdentifierLit(id) =>
          if (pos == 0) { // we are in a generator 
            if (!bindings.areContextVariablesBound) { // we are not in a filter
              useIdentifiersAsGenerator(List(id), wordNet)
                .getOrElse(useIdentifierAsGenerator(id, wordNet))
            } else {
              useIdentifierAsRelationalExprAlias(id, wordNet, bindings, data, pos, force) 
                .getOrElse {
                  if (wordNet.containsRelation(id, data.getType(0), Relation.Source))
                    useIdentifiersAsTransformation(List(id), wordNet, data, 1)
                  else
                    useIdentifierAsGenerator(id, wordNet)
                }
            }
          } else {
            useIdentifierAsRelationalExprAlias(id, wordNet, bindings, data, pos, force)
              .getOrElse(useIdentifiersAsTransformation(List(id), wordNet, data, pos))           
          }
      }
    }
  }
  
  private def useIdentifiersAsGenerator(ids: List[String], wordNet: WordNet) = {
    ids match {
      case first::second::dests =>
        wordNet.findFirstRelationByNameAndSource(second, first)
          .map(r => DataSet(wordNet.getPaths(r, first, dests)))
          .orElse(wordNet.findFirstRelationByNameAndSource(first, Relation.Source)
            .map(r => DataSet(wordNet.getPaths(r, Relation.Source, second::dests))))
      case List(head) =>
        wordNet.findFirstRelationByNameAndSource(ids.head, Relation.Source) 
          .map(r => DataSet(wordNet.getPaths(r, Relation.Source, r.argumentNames.filter(_ != Relation.Source))))        
      case Nil =>
          None        
    }
  }

  private def useIdentifierAsGenerator(id: String, wordNet: WordNet) = {
    id match {
      case "" =>
        DataSet.fromList(wordNet.words.toList)
      case "false" =>
        DataSet.fromValue(false)
      case "true" =>
        DataSet.fromValue(true)
      case id =>
        DataSet.fromOptionalValue(wordNet.getWordForm(id))                    
    }
  }

  private def useIdentifiersAsTransformation(ids: List[String], wordNet: WordNet, data: DataSet, pos: Int) = {
    val sourceType = data.getType(pos - 1)  
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
    
    wordNet.follow(data, pos, relation, source, dests)            
  }
  
  private def useIdentifierAsRelationalExprAlias(id: String, wordNet: WordNet, bindings: Bindings,
    data: DataSet, pos: Int, force: Boolean) = {
    bindings.lookupRelationalExprAlias(id).map(_.transform(wordNet, bindings, data, pos, force))      
  }
}

case class QuantifiedRelationalExpr(expr: RelationalExpr, quantifier: QuantifierLit) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, data: DataSet, pos: Int, force: Boolean) = {
    quantifier match {
      case QuantifierLit(1, Some(1)) =>
        expr.transform(wordNet, bindings, data, pos, force)
      case QuantifierLit(1, None) =>
        val pathVarNames = data.pathVars.keys.toSeq
        val stepVarNames = data.stepVars.keys.toSeq
        val pathBuffer = DataSetBuffers.createPathBuffer
        val pathVarBuffers = DataSetBuffers.createPathVarBuffers(pathVarNames)
        val stepVarBuffers = DataSetBuffers.createStepVarBuffers(stepVarNames)        
        val head = expr.transform(wordNet, bindings, data, pos, force)

        for (i <- 0 until head.pathCount) {
          val tuple = head.paths(i)
          val prefix = tuple.slice(0, tuple.size - 1)
          
          pathBuffer.append(tuple)
          pathVarNames.foreach(x => pathVarBuffers(x).append(head.pathVars(x)(i)))
          stepVarNames.foreach(x => stepVarBuffers(x).append(head.stepVars(x)(i)))          

          for (cnode <- closure(wordNet, bindings, expr, List(tuple.last), Set(tuple.last))) {
            pathBuffer.append(prefix ++ cnode)
            pathVarNames.foreach(x => pathVarBuffers(x).append(head.pathVars(x)(i)))
            stepVarNames.foreach(x => stepVarBuffers(x).append(head.stepVars(x)(i)))        
          }
        }
        
        DataSet.fromBuffers(pathBuffer, pathVarBuffers, stepVarBuffers)
      case _ =>
        throw new IllegalArgumentException("Unsupported quantifier value " + quantifier)
    }
  }
  
  private def closure(wordNet: WordNet, bindings: Bindings, expr: RelationalExpr, source: List[Any], forbidden: Set[Any]): List[List[Any]] = {
    val transformed = expr.transform(wordNet, bindings, DataSet.fromTuple(source), 1, true)
    val filtered = transformed.paths.filter { x => !forbidden.contains(x.last) }

    if (filtered.isEmpty) {
      filtered
    } else {
      val result = new ListBuffer[List[Any]]
      val newForbidden: Set[Any] = forbidden.++[Any, Set[Any]](filtered.map { x => x.last }) // TODO ugly

      result.appendAll(filtered)

      filtered.foreach { x =>
        result.appendAll(closure(wordNet, bindings, expr, x, newForbidden))
      }

      result.toList
    }
  }  
}

case class UnionRelationalExpr(lexpr: RelationalExpr, rexpr: RelationalExpr) extends RelationalExpr {
  def transform(wordNet: WordNet, bindings: Bindings, data: DataSet, pos: Int, force: Boolean) = {
    val lresult = lexpr.transform(wordNet, bindings, data, pos, force)
    val rresult = rexpr.transform(wordNet, bindings, data, pos, force)

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
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings): Boolean
}

case class OrExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = exprs.exists { x => x.satisfied(functions, wordNet, bindings) }
}

case class AndExpr(exprs: List[ConditionalExpr]) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = exprs.forall { x => x.satisfied(functions, wordNet, bindings) }
}

case class NotExpr(expr: ConditionalExpr) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = !expr.satisfied(functions, wordNet, bindings)
}

case class ComparisonExpr(op: String, lexpr: EvaluableExpr, rexpr: EvaluableExpr) extends ConditionalExpr {
  def satisfied(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val lresult = lexpr.evaluate(functions, wordNet, bindings).paths.map(_.last)
    val rresult = rexpr.evaluate(functions, wordNet, bindings).paths.map(_.last)

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
case class SynsetAllReq() extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.synsets.toList)
}

case class SynsetByExprReq(expr: EvaluableExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    val eresult = expr.evaluate(functions, wordNet, bindings)

    if (eresult.pathCount == 1 && eresult.minPathSize == 1 && eresult.maxPathSize == 1) {
      if (eresult.getType(0) == SenseType) {
        DataSet(eresult.paths.map { case List(sense: Sense) => List(wordNet.demandSynsetBySense(sense)) })
      } else if (eresult.getType(0) == StringType) {          
        DataSet(eresult.paths.flatMap { case List(wordForm: String) => wordNet.getSynsetsByWordForm(wordForm) }.map(x => List(x)))  
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

case class SenseAllReq() extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.senses.toList)
}

case class SenseByWordFormAndSenseNumberAndPosReq(word: String, num: Int, pos: String) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromOptionalValue(wordNet.getSenseByWordFormAndSenseNumberAndPos(word, num, pos))
}

case class SenseByWordFormAndSenseNumberReq(word: String, num: Int) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = DataSet.fromList(wordNet.getSensesByWordFormAndSenseNumber(word, num))
}

case class ContextByRelationalExprReq(expr: RelationalExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    expr.transform(wordNet, bindings, DataSet(List(bindings.contextVariables)), 0, false)
  }
}

case class WordFormByRegexReq(v: String) extends FunctionsFreeExpr {
  def evaluate(wordNet: WordNet) = {
    val regex = v.r
    val result = wordNet.words.filter(x => regex.findFirstIn(x).map(_ => true).getOrElse(false))

    DataSet.fromList(result.toList)
  }
}

case class ContextByReferenceReq(ref: Int) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    bindings.lookupContextVariable(ref).map(DataSet.fromValue(_))
      .getOrElse(throw new WQueryEvaluationException("Backward reference '" + List.fill(ref)("#").mkString + "' too far"))
  }
}

case class ContextByVariableReq(variable: VariableLit) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    variable match {
      case PathVariableLit(name) =>
        bindings.lookupPathVariable(name).map(DataSet.fromTuple(_))
          .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable @'" + name + "' found"))
      case StepVariableLit(name) =>
        bindings.lookupStepVariable(name).map(DataSet.fromValue(_))
          .getOrElse(throw new WQueryEvaluationException("A reference to unknown variable $'" + name + "' found"))
    }
  }
}

case class BooleanByFilterReq(cond: ConditionalExpr) extends EvaluableExpr {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = DataSet.fromValue(cond.satisfied(functions, wordNet, bindings))
}

/*
 * Variables
 */
case class GeneratorExpr(expr: EvaluableExpr, decls: List[VariableLit]) extends EvaluableExpr with VariableBindings {
  def evaluate(functions: FunctionSet, wordNet: WordNet, bindings: Bindings) = {
    bind(expr.evaluate(functions, wordNet, bindings), decls)
  }
}

sealed abstract class VariableLit(val value: String) extends Expr

case class StepVariableLit(override val value: String) extends VariableLit(value)
case class PathVariableLit(override val value: String) extends VariableLit(value)

/*
 * Literals
 */
case class DoubleQuotedLit(value: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class StringLit(value: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class IntegerLit(value: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class SequenceLit(left: Int, right: Int) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromList((left to right).toList)
}

case class FloatLit(value: Double) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class BooleanLit(value: Boolean) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class QuantifierLit(left: Int, right: Option[Int]) extends Expr

sealed abstract class IdentifierLit(val value: String) extends SelfEvaluableExpr {
  def evaluate = DataSet.fromValue(value)
}

case class NotQuotedIdentifierLit(override val value: String) extends IdentifierLit(value)
case class QuotedIdentifierLit(override val value: String) extends IdentifierLit(value)