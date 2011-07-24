package org.wquery.engine

import java.lang.reflect.Method
import scala.collection.mutable.Map
import org.wquery.model._

class Bindings(parent: Option[Bindings]) {  
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, ArcExprUnion]()
  val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()

  // syntax based structures
  val stepVariablesTypes = Map[String, Set[DataType]]()
  val pathVariablesTypes = Map[String, (AlgebraOp, Int, Int)]()

  private var contextVars = List[Any]()

  def bindPathVariable(name: String, value: List[Any]) {
    parent.map { p =>
      if (p.lookupPathVariable(name).isEmpty)
        pathVariables(name) = value
      else
        p.bindPathVariable(name, value)
    }.getOrElse {
      pathVariables(name) = value
    }
  }
  
  def bindStepVariable(name: String, value: Any) {
    parent.map { p =>
      if (p.lookupStepVariable(name).isEmpty)
        stepVariables(name) = value
      else
        p.bindStepVariable(name, value)
    }.getOrElse {
      stepVariables(name) = value
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
    parent.map { p =>
      if (p.lookupStepVariableType(name).isEmpty)
        stepVariablesTypes(name) = types
      else
        p.bindStepVariable(name, types)
    }.getOrElse {
      stepVariablesTypes(name) = types
    }
  }

  def bindPathVariableType(name: String, op: AlgebraOp, leftShift: Int, rightShift: Int) {
    parent.map { p =>
      if (p.lookupPathVariableType(name).isEmpty)
        pathVariablesTypes(name) = (op, leftShift, rightShift)
      else
        p.bindPathVariableType(name, op, leftShift, rightShift)
    }.getOrElse {
      pathVariablesTypes(name) = (op, leftShift, rightShift)
    }
  }


  def lookupStepVariableType(name: String): Option[Set[DataType]] = stepVariablesTypes.get(name).orElse(parent.flatMap(_.lookupStepVariableType(name)))

  def lookupPathVariableType(name: String): Option[(AlgebraOp, Int, Int)] = pathVariablesTypes.get(name).orElse(parent.flatMap(_.lookupPathVariableType(name)))
}

object Bindings {
  def apply() = new Bindings(None)
  
  def apply(parent: Bindings) = new Bindings(Some(parent))  
}