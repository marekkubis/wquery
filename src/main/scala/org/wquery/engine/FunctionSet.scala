package org.wquery.engine
import java.lang.reflect.Method
import org.wquery.WQueryModelException
import org.wquery.model.{TupleType, BooleanType, FloatType, IntegerType, StringType, SenseType, Sense, SynsetType, Synset, ValueType, AggregateFunction, ScalarFunction, Function, FunctionArgumentType, ArcType, Arc}
import scala.collection.mutable.Map

class FunctionSet {
  private val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()  

  def addScalarFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {
    functions += ((
      (name, args), 
      (
        new ScalarFunction(name, args, result),    
        clazz.getMethod(methodname, args.map{
          // matching for ValueType put here because scala was unable to transform in this place
          // an expression that consists of invoking a method from DataType subclasses 
          case ValueType(SynsetType) =>
            classOf[Synset] 
          case ValueType(SenseType) =>
            classOf[Sense]
          case ValueType(StringType) =>
            classOf[String]         
          case ValueType(IntegerType) =>
            classOf[Int]
          case ValueType(FloatType) =>
            classOf[Double]       
          case ValueType(BooleanType) =>
            classOf[Boolean]
          case ValueType(ArcType) =>
            classOf[Arc]                 
          case TupleType => 
            throw new IllegalArgumentException("ScalarFunction '" + 
                                                 name + "' must not take TupleType as an argument")
        }.toArray:_*)
      )
    ))    
  }
  
  def addAggregateFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, clazz: java.lang.Class[_] , methodname: String) {
    functions += ((
      (name, args), 
      (
        new AggregateFunction(name, args, result), 
        clazz.getMethod(methodname, Array.fill(args.size)(classOf[DataSet]):_*)
      )
    ))
  }  

  def removeFunction(name: String, args: List[FunctionArgumentType]) { functions -= ((name, args)) }
  
  def demandFunction(name: String, args: List[FunctionArgumentType]) = {
    if (functions contains (name, args))
      functions((name, args))  
    else
      throw new WQueryModelException("Function '" + name + "' with argument types " + args + " not found")
  }    
  
  def getFunction(name: String, args: List[FunctionArgumentType]) 
    = if (functions contains (name, args)) Some(functions((name, args))) else None    
}
