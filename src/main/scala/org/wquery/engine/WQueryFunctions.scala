package org.wquery.engine

import org.wquery.WQuery
import org.wquery.WQueryEvaluationException
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
    wquery.registerAggregateFunction(Sort, List(TupleType), TupleType, getClass, Sort)      
    wquery.registerAggregateFunction(Distinct, List(TupleType), TupleType, getClass, Distinct)      
    wquery.registerAggregateFunction(Count, List(TupleType), TupleType, getClass, Count)
    wquery.registerAggregateFunction(Sum, List(ValueType(IntegerType)), ValueType(IntegerType), getClass, "sumInt")      
    wquery.registerAggregateFunction(Sum, List(ValueType(FloatType)), ValueType(FloatType), getClass, "sumFloat")
    wquery.registerAggregateFunction(Avg, List(ValueType(IntegerType)), ValueType(FloatType), getClass, Avg)      
    wquery.registerAggregateFunction(Avg, List(ValueType(FloatType)), ValueType(FloatType), getClass, Avg)
    wquery.registerAggregateFunction(Min, List(TupleType), TupleType, getClass, Min)
    wquery.registerAggregateFunction(Max, List(TupleType), TupleType, getClass, Max)
    wquery.registerAggregateFunction(Size, List(TupleType), TupleType, getClass, Size)    
    wquery.registerScalarFunction(Abs, List(ValueType(IntegerType)), ValueType(IntegerType), classOf[Math], Abs)
    wquery.registerScalarFunction(Abs, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Abs)
    wquery.registerScalarFunction(Ceil, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Ceil)      
    wquery.registerScalarFunction(Floor, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Floor)
    wquery.registerScalarFunction(Log, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Log)
    wquery.registerScalarFunction(Power, List(ValueType(FloatType), ValueType(FloatType)), ValueType(FloatType), classOf[Math], "pow")
    wquery.registerScalarFunction(Length, List(ValueType(StringType)), ValueType(IntegerType), getClass, Length)
    wquery.registerScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType)), ValueType(StringType), getClass, Substring)
    wquery.registerScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType), ValueType(IntegerType)), ValueType(StringType), getClass, Substring)
    wquery.registerScalarFunction(Replace, List(ValueType(StringType), ValueType(StringType), ValueType(StringType)), ValueType(StringType), getClass, Replace)
    wquery.registerScalarFunction(Lower, List(ValueType(StringType)), ValueType(StringType), getClass, Lower)
    wquery.registerScalarFunction(Upper, List(ValueType(StringType)), ValueType(StringType), getClass, Upper)      
  }                                                
  
  def distinct(result: DataSet) = DataSet(result.types, result.content.distinct)
  
  def sort(result: DataSet) = DataSet(result.types, result.content.sortWith((x, y) => compare(x, y) < 0))

  def count(result: DataSet) = DataSet.fromValue(result.content.size)  

  def min(result: DataSet) = {
    val sresult = sort(result)
    DataSet(sresult.types, List(sresult.content.head))    
  }  
  
  def max(result: DataSet) = {
    val sresult = sort(result)
    DataSet(sresult.types, List(sresult.content.last))    
  }
    
  def sumInt(result: DataSet) = {
    var sum: Int = 0
    
    for (tuple <- result.content) {
        sum += tuple.head.asInstanceOf[Int]
    }
  
    DataSet.fromValue(sum)
  }
  
  def sumFloat(result: DataSet) = {
    var sum: Double = 0
    
    for (tuple <- result.content) {
        sum += tuple.head.asInstanceOf[Double]
    }
  
    DataSet.fromValue(sum)
  }  
  
  def avg(result: DataSet) = {
    if (result.types.head == IntegerType)
      DataSet.fromValue(sumInt(result).content.head.head.asInstanceOf[Int].toDouble / result.content.size)
    else if (result.types.head == FloatType)
      DataSet.fromValue(sumFloat(result).content.head.head.asInstanceOf[Double] / result.content.size)
    else 
      throw new WQueryEvaluationException("Function 'avg' cannot compute average for type " + result.types.head)  
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
            val res = compare(left.head, right.head)
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
