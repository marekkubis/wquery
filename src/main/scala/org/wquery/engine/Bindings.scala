package org.wquery.engine

import java.lang.reflect.Method
import scala.collection.mutable.Map
import org.wquery.model._

class Bindings(parent: Option[Bindings], updatesParent: Boolean) {
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, ArcExprUnion]()
  val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()

  // syntax based structures
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()

  private var contextVars = List[Any]()

  def bindStepVariable(name: String, value: Any) {
    if (updatesParent && parent.map(_.lookupStepVariable(name).isDefined).getOrElse(false)) {
      parent.get.bindStepVariable(name, value)
    } else {
      stepVariables(name) = value
    }
  }

  def bindPathVariable(name: String, value: List[Any]) {
    if (updatesParent && parent.map(_.lookupPathVariable(name).isDefined).getOrElse(false)) {
      parent.get.bindPathVariable(name, value)
    } else {
      pathVariables(name) = value
    }
  }

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

  def bindContextVariables(vars: List[Any]) = contextVars = vars
    
  def lookupPathVariable(name: String): Option[List[Any]] = pathVariables.get(name).orElse(parent.flatMap(_.lookupPathVariable(name)))
   
  def lookupStepVariable(name: String): Option[Any] = stepVariables.get(name).orElse(parent.flatMap(_.lookupStepVariable(name)))

  def lookupFunction(name: String, args: List[FunctionArgumentType]): Option[(Function, Method)] = functions.get(name, args).orElse(parent.flatMap(_.lookupFunction(name, args)))  
  
  def lookupContextVariable(pos: Int): Option[Any] = if (contextVars.size - 1 - pos >= 0) Some(contextVars(contextVars.size - 1 - pos)) else parent.flatMap(_.lookupContextVariable(pos))
  
  def contextVariables = contextVars

  def areContextVariablesBound = contextVars != Nil

  def contextVariableType(pos: Int) = DataType(contextVars(contextVars.size - 1 - pos))

  // syntax based ops
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

  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name).orElse(parent.flatMap(_.lookupStepVariableType(name)))

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name).orElse(parent.flatMap(_.lookupPathVariableType(name)))
}

object Bindings {
  def apply() = new Bindings(None, false)
  
  def apply(parent: Bindings, updatesParent: Boolean) = new Bindings(Some(parent), updatesParent)
}