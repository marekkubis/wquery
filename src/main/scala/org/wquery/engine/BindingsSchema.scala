package org.wquery.engine

import java.lang.reflect.Method
import scala.collection.mutable.Map
import org.wquery.model._
import org.wquery.WQueryEvaluationException

class BindingsSchema(val parent: Option[BindingsSchema], updatesParent: Boolean) {
  val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()
  private var contextOp: Option[AlgebraOp] = None

  def bindFunction(function: Function, clazz: java.lang.Class[_] , methodName: String) {
    val method = (function match {
      case ScalarFunction(name, args, result) =>
        clazz.getMethod(methodName, args.map{ 
          case ValueType(basicType) =>
            basicType.associatedClass
          case TupleType => 
            throw new IllegalArgumentException("Scalar function '" + name + "' must not take TupleType as an argument")
        }.toArray:_*)      
      case AggregateFunction(name, args, result) =>
        clazz.getMethod(methodName, Array.fill(args.size)(classOf[DataSet]):_*)
    })
    
    functions((function.name, function.args)) = (function, method)
  }

  def bindStepVariableType(name: String, types: Set[DataType]) {
    if (updatesParent && parent.map(_.lookupStepVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindStepVariableType(name, types)
    } else {
      stepVariablesTypes(name) = types
    }
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    if (updatesParent && parent.map(_.lookupPathVariableType(name).isDefined).getOrElse(false)) {
      parent.get.bindPathVariableType(name, op, leftShift, rightShift)
    } else {
      pathVariablesTypes(name) = (op, leftShift, rightShift)
    }
  }

  def bindContextOp(op: AlgebraOp) = contextOp = Some(op)

  def lookupFunction(name: String, args: List[FunctionArgumentType]): Option[(Function, Method)] = functions.get(name, args).orElse(parent.flatMap(_.lookupFunction(name, args)))

  def lookupStepVariableType(name: String): Option[Set[DataType]] = {
    stepVariablesTypes.get(name).orElse(parent.flatMap(_.lookupStepVariableType(name)))
  }

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name).orElse(parent.flatMap(_.lookupPathVariableType(name)))

  def lookupContextVariableType(pos: Int): Set[DataType] = {
    contextOp.map(_.rightType(pos))
      .getOrElse(parent.map(_.lookupContextVariableType(pos))
        .getOrElse{throw new WQueryEvaluationException("Operator # placed outside of a context")})
  }

  def areContextVariablesBound: Boolean = contextOp.isDefined || parent.map(_.areContextVariablesBound).getOrElse(false)

  def union(that: BindingsSchema) = {
    val sum = parent match {
      case Some(parent) =>
        that.parent.map(throw new RuntimeException("An attempt to merge two bindings with defined parents"))
          .getOrElse(BindingsSchema(parent, false))
      case None =>
        that.parent.map(BindingsSchema(_, false)).getOrElse(BindingsSchema())
    }

    // TODO ugly loops
    for (v <- functions.values)
      sum.bindFunction(v._1, v._2.getDeclaringClass, v._2.getName)

    for (v <- that.functions.values)
      sum.bindFunction(v._1, v._2.getDeclaringClass, v._2.getName)

    for ((k, v) <- stepVariablesTypes)
      sum.bindStepVariableType(k, v)

    for ((k, v) <- that.stepVariablesTypes)
      sum.bindStepVariableType(k, v)

    for ((k, v) <- pathVariablesTypes)
      sum.bindPathVariableType(k, v._1, v._2, v._3)

    for ((k, v) <- that.pathVariablesTypes)
      sum.bindPathVariableType(k, v._1, v._2, v._3)

    sum
  }
}

object BindingsSchema {
  def apply() = new BindingsSchema(None, false)
  
  def apply(parent: BindingsSchema, updatesParent: Boolean) = new BindingsSchema(Some(parent), updatesParent)
}