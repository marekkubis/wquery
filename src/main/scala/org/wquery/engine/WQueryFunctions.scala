package org.wquery.engine

import org.wquery.WQueryEvaluationException
import org.wquery.model._

/**
 * WQuery built-in functions
 */
object WQueryFunctions {
  val Sort = "sort"
  val Count = "count"
  val Last = "last"
  val Sum = "sum"
  val Avg = "avg"
  val Min = "min"  
  val Max = "max"    
  val Distinct = "distinct"
  val Shortest = "shortest"
  val Longest= "longest"
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
  val StringLength = "string_length"
  val Empty = "empty"

  val functions = List(
    (AggregateFunction(Sort, List(TupleType), TupleType), getClass, Sort),      
    (AggregateFunction(Distinct, List(TupleType), TupleType), getClass, Distinct),      
    (AggregateFunction(Count, List(TupleType), ValueType(IntegerType)), getClass, Count),
    (AggregateFunction(Last, List(TupleType), TupleType), getClass, Last),    
    (AggregateFunction(Sum, List(ValueType(IntegerType)), ValueType(IntegerType)), getClass, "sumInt"),      
    (AggregateFunction(Sum, List(ValueType(FloatType)), ValueType(FloatType)), getClass, "sumFloat"),
    (AggregateFunction(Avg, List(ValueType(IntegerType)), ValueType(FloatType)), getClass, Avg),      
    (AggregateFunction(Avg, List(ValueType(FloatType)), ValueType(FloatType)), getClass, Avg),
    (AggregateFunction(Min, List(TupleType), TupleType), getClass, Min),
    (AggregateFunction(Max, List(TupleType), TupleType), getClass, Max),
    (AggregateFunction(Shortest, List(TupleType), TupleType), getClass, Shortest),
    (AggregateFunction(Longest, List(TupleType), TupleType), getClass, Longest),
    (AggregateFunction(Size, List(TupleType), ValueType(IntegerType)), getClass, Size),
    (AggregateFunction(Length, List(TupleType), ValueType(IntegerType)), getClass, Length),
    (AggregateFunction(Empty, List(TupleType), ValueType(BooleanType)), getClass, Empty),
    (ScalarFunction(Abs, List(ValueType(IntegerType)), ValueType(IntegerType)), classOf[Math], Abs),
    (ScalarFunction(Abs, List(ValueType(FloatType)), ValueType(FloatType)), classOf[Math], Abs),
    (ScalarFunction(Ceil, List(ValueType(FloatType)), ValueType(FloatType)), classOf[Math], Ceil),      
    (ScalarFunction(Floor, List(ValueType(FloatType)), ValueType(FloatType)), classOf[Math], Floor),
    (ScalarFunction(Log, List(ValueType(FloatType)), ValueType(FloatType)), classOf[Math], Log),
    (ScalarFunction(Power, List(ValueType(FloatType), ValueType(FloatType)), ValueType(FloatType)), classOf[Math], "pow"),
    (ScalarFunction(StringLength, List(ValueType(StringType)), ValueType(IntegerType)), getClass, StringLength),
    (ScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType)), ValueType(StringType)), getClass, Substring),
    (ScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType), ValueType(IntegerType)), ValueType(StringType)), getClass, Substring),
    (ScalarFunction(Replace, List(ValueType(StringType), ValueType(StringType), ValueType(StringType)), ValueType(StringType)), getClass, Replace),
    (ScalarFunction(Lower, List(ValueType(StringType)), ValueType(StringType)), getClass, Lower),
    (ScalarFunction(Upper, List(ValueType(StringType)), ValueType(StringType)), getClass, Upper)     
  )                                              
  
  def distinct(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.distinct)
  
  def sort(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.sortWith((x, y) => compare(x._1, y._1) < 0))

  def count(dataSet: DataSet) = DataSet.fromValue(dataSet.paths.size)
  
  def last(dataSet: DataSet) = DataSet(dataSet.paths.map(x => List(x.last)))

  def min(dataSet: DataSet) = sort(dataSet).paths.headOption.map(x => DataSet(List(x))).getOrElse(DataSet.empty)
    
  def max(dataSet: DataSet) = sort(dataSet).paths.lastOption.map(x => DataSet(List(x))).getOrElse(DataSet.empty)

  def shortest(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.minTupleSize))

  def longest(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.maxTupleSize))

  def sumInt(dataSet: DataSet) = {
    var sum: Int = 0
    
    for (tuple <- dataSet.paths) {
        sum += tuple.head.asInstanceOf[Int]
    }
  
    DataSet.fromValue(sum)
  }
  
  def sumFloat(dataSet: DataSet) = {
    var sum: Double = 0
    
    for (tuple <- dataSet.paths) {
        tuple match {
            case List(value:Double) =>
                sum += value
            case List(value:Int) =>
                sum += value
            case obj =>
                throw new WQueryEvaluationException("An atempt to sum nonnumeric value " + obj)
        }
    }
  
    DataSet.fromValue(sum)
  }  
  
  def avg(dataSet: DataSet) = {      
    if (dataSet.minTupleSize == 1 && dataSet.maxTupleSize == 1) {
        val types = dataSet.rightType(0)

        if (types == Set(IntegerType))
          DataSet.fromValue(sumInt(dataSet).paths.head.head.asInstanceOf[Int].toDouble / dataSet.paths.size)
        else if (DataType.numeric.contains(types))
          DataSet.fromValue(sumFloat(dataSet).paths.head.head.asInstanceOf[Double] / dataSet.paths.size)      
        else 
          throw new WQueryEvaluationException("Function 'avg' can compute average for numeric types only")
    } else {
      throw new WQueryEvaluationException("Function 'avg' requires single element tuples")        
    }
  }
  
  def size(dataSet: DataSet) = DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))  

  def length(dataSet: DataSet) = DataSet(dataSet.paths.map{path => List(path.size)})  

  def empty(dataSet: DataSet) = DataSet.fromValue(dataSet.isEmpty)

  def string_length(word: String) = word.size  
  
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
      case (left: Arc, right: Arc) =>
        if (left.relation.name != right.relation.name)
            left.relation.name compare right.relation.name
        else if (left.from != right.from)
            left.from compare right.from
        else
            left.to compare right.to        
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
