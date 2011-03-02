package org.wquery.engine

import org.wquery.model.BasicType
import java.lang.reflect.Method
import org.wquery.model.{Function, FunctionArgumentType, ScalarFunction, ValueType, SynsetType, Synset, SenseType, Sense, StringType, IntegerType, FloatType, BooleanType, ArcType, AggregateFunction, DataSet, Arc, TupleType}
import scala.collection.mutable.Map

class Bindings(parent: Option[Bindings]) {  
  val pathVariables = Map[String, List[Any]]()
  val stepVariables = Map[String, Any]()  
  val relationalExprAliases = Map[String, RelationalExpr]()
  val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()  
  
  private var contextVars = List[Any]()

  def bindPathVariable(name: String, value: List[Any]) = (pathVariables(name) = value)

  def bindStepVariable(name: String, value: Any) = (stepVariables(name) = value) 

  def bindRelationalExprAlias(name: String, value: RelationalExpr) = (relationalExprAliases(name) = value)
  
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
  
  def lookupRelationalExprAlias(name: String): Option[RelationalExpr] = relationalExprAliases.get(name).orElse(parent.flatMap(_.lookupRelationalExprAlias(name)))
  
  def lookupFunction(name: String, args: List[FunctionArgumentType]): Option[(Function, Method)] = functions.get(name, args).orElse(parent.flatMap(_.lookupFunction(name, args)))  
  
  def lookupContextVariable(pos: Int): Option[Any] = if (contextVars.size - pos >= 0) Some(contextVars(contextVars.size - pos)) else parent.flatMap(_.lookupContextVariable(pos))  
  
  def areContextVariablesBound = contextVars != Nil
  
  def contextVariables = contextVars
}

object Bindings {
  def apply() = new Bindings(None)
  
  def apply(parent: Bindings) = new Bindings(Some(parent))  
}