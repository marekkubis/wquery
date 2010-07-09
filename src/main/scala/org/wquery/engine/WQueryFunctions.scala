package org.wquery.engine

import org.wquery.model._
import java.lang.reflect.Method

/**
 * WQuery built-in functions
 */
object WQueryFunctions {
  val Sort = "sort"
  val Count = "count"
  val Sum = "sum"
  val Avg = "avg"
  val Min = "min"  
  val Max = "max"    
  val Distinct = "distinct"
  val Size = "size"
  val Compare = "compare"
  val Abs = "abs"
  val Ceil = "ceil"
  val Floor = "floor"
  val Log = "log"
  val Power = "power"
  val Length = "length"
  val Substring = "substring"
  val Replace = "replace"
  val Lower = "lower"
  val Upper = "upper"
    
  def registerFunctionsIn(wquery: WQuery) {
    wquery.registerFunction(Sort, List(TupleType), TupleType, AggregateType, getClass, Sort)      
    wquery.registerFunction(Distinct, List(TupleType), TupleType, AggregateType, getClass, Distinct)      
    wquery.registerFunction(Count, List(TupleType), TupleType, AggregateType, getClass, Count)
    wquery.registerFunction(Sum, List(ValueType(IntegerType)), ValueType(IntegerType), AggregateType, getClass, "sumInt")      
    wquery.registerFunction(Sum, List(ValueType(FloatType)), ValueType(FloatType), AggregateType, getClass, "sumFloat")
    wquery.registerFunction(Avg, List(ValueType(IntegerType)), ValueType(FloatType), AggregateType, getClass, Avg)      
    wquery.registerFunction(Avg, List(ValueType(FloatType)), ValueType(FloatType), AggregateType, getClass, Avg)
    wquery.registerFunction(Min, List(TupleType), TupleType, AggregateType, getClass, Min)
    wquery.registerFunction(Max, List(TupleType), TupleType, AggregateType, getClass, Max)
    wquery.registerFunction(Size, List(TupleType), TupleType, AggregateType, getClass, Size)    
    wquery.registerFunction(Abs, List(ValueType(IntegerType)), ValueType(IntegerType), ScalarType, classOf[Math], Abs)
    wquery.registerFunction(Abs, List(ValueType(FloatType)), ValueType(FloatType), ScalarType, classOf[Math], Abs)
    wquery.registerFunction(Ceil, List(ValueType(FloatType)), ValueType(FloatType), ScalarType, classOf[Math], Ceil)      
    wquery.registerFunction(Floor, List(ValueType(FloatType)), ValueType(FloatType), ScalarType, classOf[Math], Floor)
    wquery.registerFunction(Log, List(ValueType(FloatType)), ValueType(FloatType), ScalarType, classOf[Math], Log)
    wquery.registerFunction(Power, List(ValueType(FloatType), ValueType(FloatType)), ValueType(FloatType), ScalarType, classOf[Math], "pow")
    wquery.registerFunction(Length, List(ValueType(StringType)), ValueType(IntegerType), ScalarType, getClass, Length)
    wquery.registerFunction(Substring, List(ValueType(StringType), ValueType(IntegerType)), ValueType(StringType), ScalarType, getClass, Substring)
    wquery.registerFunction(Substring, List(ValueType(StringType), ValueType(IntegerType), ValueType(IntegerType)), ValueType(StringType), ScalarType, getClass, Substring)
    wquery.registerFunction(Replace, List(ValueType(StringType), ValueType(StringType), ValueType(StringType)), ValueType(StringType), ScalarType, getClass, Replace)
    wquery.registerFunction(Lower, List(ValueType(StringType)), ValueType(StringType), ScalarType, getClass, Lower)
    wquery.registerFunction(Upper, List(ValueType(StringType)), ValueType(StringType), ScalarType, getClass, Upper)      
  }                                                
  
  def distinct(result: DataSet) = DataSet(result.types, result.content.removeDuplicates)
  
  def sort(result: DataSet) = DataSet(result.types, result.content.sort((x, y) => compare(x, y) < 0))

  def count(result: DataSet) = DataSet.fromValue(result.content.size)  

  def min(result: DataSet) = {
    val sresult = sort(result)
    DataSet(sresult.types, List(sresult.content.first))    
  }  
  
  def max(result: DataSet) = {
    val sresult = sort(result)
    DataSet(sresult.types, List(sresult.content.last))    
  }
    
  def sumInt(result: DataSet) = {
    var sum: Int = 0
    
    for (tuple <- result.content) {
        sum += tuple.first.asInstanceOf[Int]
    }
  
    DataSet.fromValue(sum)
  }
  
  def sumFloat(result: DataSet) = {
    var sum: Double = 0
    
    for (tuple <- result.content) {
        sum += tuple.first.asInstanceOf[Double]
    }
  
    DataSet.fromValue(sum)
  }  
  
  def avg(result: DataSet) = {
    if (result.types.first == IntegerType)
      DataSet.fromValue(sumInt(result).content.first.first.asInstanceOf[Int].toDouble / result.content.size)
    else if (result.types.first == FloatType)
      DataSet.fromValue(sumFloat(result).content.first.first.asInstanceOf[Double] / result.content.size)
    else 
      throw new WQueryEvaluationException("Function 'avg' cannot compute average for type " + result.types.first)  
  }
  
  def size(result: DataSet) = DataSet(List(IntegerType), result.content.map{x => List(x.size)})  
  
  def length(word: String) = word.length
  
  def substring(word: String, index: Int) = if (index < word.length) word.substring(index) else ""

  def substring(word: String, left: Int, right: Int) = {
    if (left < word.length) { 
      if (right <= word.length) {
        word.substring(left, right)
      } else {
        word.substring(left)
      }
    } else {
      ""
    }
  }
  
  def replace(word: String, pattern: String, replacement: String) = word.replaceFirst(pattern, replacement)  
  
  def lower(word: String) = word.toLowerCase
  
  def upper(word: String) = word.toUpperCase
  
  def compare(left: Any, right: Any): Int = {
    (left, right) match {      
      case (left: Synset, right: Synset) =>
        left.id compare right.id
      case (left: Sense, right: Sense) =>
        left.id compare right.id
      case (left: String, right: String) =>
        left compare right
      case (left: Int, right: Int) =>
        left compare right
      case (left: Int, right: Double) =>
        left.doubleValue compare right
      case (left: Double, right: Int) =>
        left compare right      
      case (left: Double, right: Double) =>
        left compare right      
      case (left: Boolean, right: Boolean) =>
        left compare right      
      case (left: List[_], right: List[_]) =>      
        if (left.isEmpty) {
          if (right.isEmpty) 0 else -1
        } else {
          if (right.isEmpty) {
            1
          } else {
            val res = compare(left.first, right.first)
            if (res != 0) {
              res
            } else {
              compare(left.tail, right.tail)          
            }
          }
        }
      case _ =>
        throw new IllegalArgumentException("Objects " + left + " and " + right + " cannot be compared")              
    }     
  }
}
