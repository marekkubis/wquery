package org.wquery.engine

import java.lang.reflect.Method
import scala.collection.mutable.Map
import org.wquery.WQueryModelException
import org.wquery.model.FunctionArgumentType
import org.wquery.model.Function
import org.wquery.model.AggregateType
import org.wquery.model.FunctionType
import org.wquery.model.ScalarType
import org.wquery.model.ValueType
import org.wquery.model.Synset
import org.wquery.model.SynsetType
import org.wquery.model.Sense
import org.wquery.model.SenseType
import org.wquery.model.StringType
import org.wquery.model.IntegerType
import org.wquery.model.FloatType
import org.wquery.model.BooleanType
import org.wquery.model.TupleType
import org.wquery.model.FunctionArgumentType

class FunctionSet {
  private val functions = Map[(String, List[FunctionArgumentType]), (Function, Method)]()  

  def addFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType, 
                  ftype: FunctionType, clazz: java.lang.Class[_] , methodname: String) {
    ftype match {
      case ScalarType =>
        functions += ((
          (name, args), 
          (
            new Function(name, args, result, ftype),    
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
              case TupleType => 
                throw new IllegalArgumentException("Scalar function '" + 
                                                     name + "' shall not take TupleType as an argument")
            }.toArray:_*)
          )
        ))
      case AggregateType =>
        functions += ((
          (name, args), 
          (
            new Function(name, args, result, ftype), 
            clazz.getMethod(methodname, Array.make(args.size, classOf[DataSet]):_*)
          )
        ))
    }
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
