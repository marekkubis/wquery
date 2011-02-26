package org.wquery.engine
import org.wquery.{WQueryEvaluationException, WQuery}
import org.wquery.model.{ValueType, TupleType, IntegerType, FloatType, StringType, Synset, Sense, Arc, DataSet}

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
    
  def registerFunctionsIn(wquery: WQuery) {
    wquery.registerAggregateFunction(Sort, List(TupleType), TupleType, getClass, Sort)      
    wquery.registerAggregateFunction(Distinct, List(TupleType), TupleType, getClass, Distinct)      
    wquery.registerAggregateFunction(Count, List(TupleType), TupleType, getClass, Count)
    wquery.registerAggregateFunction(Last, List(TupleType), TupleType, getClass, Last)    
    wquery.registerAggregateFunction(Sum, List(ValueType(IntegerType)), ValueType(IntegerType), getClass, "sumInt")      
    wquery.registerAggregateFunction(Sum, List(ValueType(FloatType)), ValueType(FloatType), getClass, "sumFloat")
    wquery.registerAggregateFunction(Avg, List(ValueType(IntegerType)), ValueType(FloatType), getClass, Avg)      
    wquery.registerAggregateFunction(Avg, List(ValueType(FloatType)), ValueType(FloatType), getClass, Avg)
    wquery.registerAggregateFunction(Min, List(TupleType), TupleType, getClass, Min)
    wquery.registerAggregateFunction(Max, List(TupleType), TupleType, getClass, Max)
    wquery.registerAggregateFunction(Size, List(TupleType), TupleType, getClass, Size) 
    wquery.registerAggregateFunction(Length, List(TupleType), TupleType, getClass, Length)    
    wquery.registerScalarFunction(Abs, List(ValueType(IntegerType)), ValueType(IntegerType), classOf[Math], Abs)
    wquery.registerScalarFunction(Abs, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Abs)
    wquery.registerScalarFunction(Ceil, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Ceil)      
    wquery.registerScalarFunction(Floor, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Floor)
    wquery.registerScalarFunction(Log, List(ValueType(FloatType)), ValueType(FloatType), classOf[Math], Log)
    wquery.registerScalarFunction(Power, List(ValueType(FloatType), ValueType(FloatType)), ValueType(FloatType), classOf[Math], "pow")
    wquery.registerScalarFunction(StringLength, List(ValueType(StringType)), ValueType(IntegerType), getClass, StringLength)    
    wquery.registerScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType)), ValueType(StringType), getClass, Substring)
    wquery.registerScalarFunction(Substring, List(ValueType(StringType), ValueType(IntegerType), ValueType(IntegerType)), ValueType(StringType), getClass, Substring)
    wquery.registerScalarFunction(Replace, List(ValueType(StringType), ValueType(StringType), ValueType(StringType)), ValueType(StringType), getClass, Replace)
    wquery.registerScalarFunction(Lower, List(ValueType(StringType)), ValueType(StringType), getClass, Lower)
    wquery.registerScalarFunction(Upper, List(ValueType(StringType)), ValueType(StringType), getClass, Upper)      
  }                                                
  
  def distinct(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.distinct)
  
  def sort(dataSet: DataSet) = DataSet.fromBoundPaths(dataSet.toBoundPaths.sortWith((x, y) => compare(x._1, y._1) < 0))

  def count(dataSet: DataSet) = DataSet.fromValue(dataSet.paths.size)
  
  def last(dataSet: DataSet) = DataSet(dataSet.paths.map(x => List(x.last)))

  def min(dataSet: DataSet) = DataSet(List(sort(dataSet).paths.head))
  
  def max(dataSet: DataSet) = DataSet(List(sort(dataSet).paths.last))
    
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
    if (dataSet.minPathSize == 1 && dataSet.maxPathSize == 1) {  
        if (dataSet.getType(0) == IntegerType)
          DataSet.fromValue(sumInt(dataSet).paths.head.head.asInstanceOf[Int].toDouble / dataSet.paths.size)
        else if (dataSet.isNumeric(0))
          DataSet.fromValue(sumFloat(dataSet).paths.head.head.asInstanceOf[Double] / dataSet.paths.size)      
        else 
          throw new WQueryEvaluationException("Function 'avg' can compute average for numeric types only")
    } else {
      throw new WQueryEvaluationException("Function 'avg' requires single element tuples")        
    }
  }
  
  def size(dataSet: DataSet) = DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))  

  def length(dataSet: DataSet) = DataSet(dataSet.paths.map{path => List(path.size)})  
  
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
