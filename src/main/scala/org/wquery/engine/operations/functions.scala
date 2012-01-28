package org.wquery.engine.operations

import collection.mutable.ListBuffer
import java.lang.reflect.Method
import scalaz._
import Scalaz._
import org.wquery.model._

abstract class Function(val name: String) {
  def accepts(args: AlgebraOp): Boolean
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(args: AlgebraOp, pos: Int): Set[DataType]
  def rightType(args: AlgebraOp, pos: Int): Set[DataType]
  def minTupleSize(args: AlgebraOp): Int
  def maxTupleSize(args: AlgebraOp): Option[Int]
  def bindingsPattern(args: AlgebraOp): BindingsPattern
  def maxCount(args: AlgebraOp, wordNet: WordNetSchema): Option[BigInt]
  override def toString = name
}

abstract class DataSetFunction(override val name: String) extends Function(name) {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings): DataSet

  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings) = {
    evaluate(args.evaluate(wordNet, bindings), wordNet, bindings)
  }
}

trait AcceptsAll {
  def accepts(args: AlgebraOp) = true
}

trait AcceptsNumbers {
  def accepts(args: AlgebraOp) = args.minTupleSize == 1 && args.maxTupleSize == Some(1) && args.rightType(0).subsetOf(DataType.numeric)
}

trait AcceptsTypes {
  def argumentTypes: List[Set[DataType]]

  def accepts(args: AlgebraOp) = {
    args.minTupleSize == argumentTypes.size && args.maxTupleSize == Some(argumentTypes.size) &&
      argumentTypes.zipWithIndex.forall{ case (dataType, pos) => args.leftType(pos) == dataType }
  }
}

trait PreservesBindingsPattern {
  def bindingsPattern(args: AlgebraOp) = args.bindingsPattern
}

trait ClearsBindingsPattern {
  def bindingsPattern(args: AlgebraOp) = BindingsPattern()
}

trait ReturnsValueSet {
  def returnType(args: AlgebraOp): Set[DataType]

  def minTupleSize(args: AlgebraOp) = 1

  def maxTupleSize(args: AlgebraOp) = Some(1)

  def leftType(args: AlgebraOp, pos: Int): Set[DataType] = if (pos == 0) returnType(args) else Set.empty

  def rightType(args: AlgebraOp, pos: Int): Set[DataType] = if (pos == 0) returnType(args) else Set.empty
}

trait ReturnsSingleTuple {
  def maxCount(args: AlgebraOp, wordNet: WordNetSchema) = some(BigInt(1))
}

trait ReturnsDataSetOfSimilarSize {
  def maxCount(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet)
}

trait ReturnsSingleValue extends ReturnsValueSet with ReturnsSingleTuple

trait ReturnsValueSetOfSimilarSize extends ReturnsValueSet with ReturnsDataSetOfSimilarSize

trait PreservesTypes {
  def leftType(args: AlgebraOp, pos: Int) = args.leftType(pos)

  def rightType(args: AlgebraOp, pos: Int) = args.rightType(pos)
}

trait PreservesTupleSizes {
  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object DistinctFunction extends DataSetFunction("distinct") with AcceptsAll
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    dataSet.distinct
  }

  def maxCount(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet)
}

object SortFunction extends DataSetFunction("sort") with AcceptsAll with ReturnsDataSetOfSimilarSize
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.sortBy(x => x._1)(WQueryListOrdering))
  }
}

object CountFunction extends DataSetFunction("count") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.paths.size)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LastFunction extends DataSetFunction("last") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(x => List(x.last)))
  }

  def returnType(args: AlgebraOp) = args.rightType(0)
}

object IntegerSumFunction extends DataSetFunction("sum") with AcceptsTypes with ReturnsSingleValue
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    var sum: Int = 0

    for (tuple <- dataSet.paths) {
        sum += tuple.head.asInstanceOf[Int]
    }

    DataSet.fromValue(sum)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object FloatSumFunction extends DataSetFunction("sum") with AcceptsNumbers with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    var sum: Double = 0

    for (tuple <- dataSet.paths) {
      (tuple: @unchecked) match {
        case List(value:Double) =>
          sum += value
        case List(value:Int) =>
          sum += value
      }
    }

    DataSet.fromValue(sum)
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

object AvgFunction extends Function("avg") with AcceptsNumbers with ReturnsSingleValue
 with ClearsBindingsPattern {
  def evaluate(op: AlgebraOp, wordNet: WordNet, bindings: Bindings) = {
    val dataSet = op.evaluate(wordNet, bindings)

    if (op.rightType(0) == Set(IntegerType)) {
      DataSet.fromValue(IntegerSumFunction.evaluate(dataSet, wordNet, bindings)
        .paths.head.head.asInstanceOf[Int].toDouble / dataSet.paths.size)
    } else {
      DataSet.fromValue(FloatSumFunction.evaluate(dataSet, wordNet, bindings)
        .paths.head.head.asInstanceOf[Double] / dataSet.paths.size)
    }
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

object MinFunction extends DataSetFunction("min") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
   SortFunction.evaluate(dataSet, wordNet, bindings)
     .paths.headOption.map(x => DataSet(List(x))).orZero
  }
}

object MaxFunction extends DataSetFunction("max") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
   SortFunction.evaluate(dataSet, wordNet, bindings)
     .paths.lastOption.map(x => DataSet(List(x))).orZero
  }
}

object ShortestFunction extends DataSetFunction("shortest") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.minTupleSize))
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = Some(args.minTupleSize)
}

object LongestFunction extends DataSetFunction("longest") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.maxTupleSize.get))
  }

  def minTupleSize(args: AlgebraOp) = args.maxTupleSize.getOrElse(args.minTupleSize)

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object SizeFunction extends DataSetFunction("size") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LengthFunction extends DataSetFunction("length") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map{path => List(path.size)})
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object EmptyFunction extends DataSetFunction("empty") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.isEmpty)
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object StringLengthFunction extends DataSetFunction("string_length") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[String].size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object SubstringFromFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(StringType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, index: Int) =>
        List[Any](if (index < word.size) word.substring(index) else "")
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object SubstringFromToFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(StringType), Set(IntegerType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, left: Int, right: Int) =>
        List[Any](
          if (left < word.length) {
            if (right <= word.length)
              word.substring(left, right)
            else word.substring(left)
          } else {
            ""
          }
        )
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object ReplaceFunction extends DataSetFunction("replace") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(StringType), Set(StringType), Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, pattern: String, replacement: String) =>
        List[Any](word.replaceFirst(pattern, replacement))
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object LowerFunction extends DataSetFunction("lower") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String) =>
        List[Any](word.toLowerCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object UpperFunction extends DataSetFunction("upper") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String) =>
        List[Any](word.toUpperCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object RangeFunction extends DataSetFunction("range") with AcceptsTypes with ReturnsValueSet
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(IntegerType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.flatMap(tuple => (tuple: @unchecked) match {
      case List(left: Int, right: Int) =>
        (left to right).map(List(_))
    }))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)

  def maxCount(args: AlgebraOp, wordNet: WordNetSchema) = args match {
    // infer the range suze from the arguments structure or assume that it can be from MIN_INT to MAX_INT
    case JoinOp(ConstantOp(leftSet), ConstantOp(rightSet)) =>
      if (leftSet.containsSingleValue && rightSet.containsSingleValue)
        some(rightSet.paths.head.head.asInstanceOf[Int] - leftSet.paths.head.head.asInstanceOf[Int] + 1)
      else
        some(BigInt(Int.MaxValue)*2)
    case _ =>
      some(BigInt(Int.MaxValue)*2)
  }
}

object IntFunction extends DataSetFunction("int") with AcceptsAll with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.collect{
      case List(value: Double) => List(value.toInt)
      case path @ List(value: Int) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object FloatFunction extends DataSetFunction("float") with AcceptsAll with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.collect{
      case List(value: Int) => List(value.toDouble)
      case path @ List(value: Double) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

abstract class JavaMethod(override val name: String, method: Method) extends Function(name) {
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings) = {
    val argsValues = args.evaluate(wordNet, bindings)
    val buffer = new ListBuffer[List[Any]]()
    val invocationArgs = new Array[AnyRef](method.getParameterTypes.size)

    for (tuple <- argsValues.paths) {
      for (i <- 0 until invocationArgs.size)
        invocationArgs(i) = tuple(i).asInstanceOf[AnyRef]

      buffer.append(List(method.invoke(null, invocationArgs: _*)))
    }

    DataSet(buffer.toList)
  }
}

object Functions {
  private val functions = scala.collection.mutable.Map[String, List[Function]]()

  def registerFunction(function: Function) {
    functions.get(function.name).map(l => functions(function.name) = (l :+ function))
      .getOrElse(functions(function.name) = List(function))
  }

  def findFunctionsByName(name: String) = functions.get(name)

  registerFunction(DistinctFunction)
  registerFunction(SortFunction)
  registerFunction(CountFunction)
  registerFunction(LastFunction)
  registerFunction(IntegerSumFunction)
  registerFunction(FloatSumFunction)
  registerFunction(AvgFunction)
  registerFunction(MinFunction)
  registerFunction(MaxFunction)
  registerFunction(ShortestFunction)
  registerFunction(LongestFunction)
  registerFunction(SizeFunction)
  registerFunction(LengthFunction)
  registerFunction(EmptyFunction)
  registerFunction(StringLengthFunction)
  registerFunction(SubstringFromFunction)
  registerFunction(SubstringFromToFunction)
  registerFunction(ReplaceFunction)
  registerFunction(LowerFunction)
  registerFunction(UpperFunction)
  registerFunction(RangeFunction)
  registerFunction(IntFunction)
  registerFunction(FloatFunction)

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", IntegerType.associatedClass))
   with AcceptsTypes with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def argumentTypes = List(Set(IntegerType))
    def returnType(args: AlgebraOp) = Set(IntegerType)
  })

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("ceil", classOf[Math].getMethod("ceil", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("floor", classOf[Math].getMethod("floor", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("log", classOf[Math].getMethod("log", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("power", classOf[Math].getMethod("pow", FloatType.associatedClass, FloatType.associatedClass))
   with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
    def accepts(args: AlgebraOp) = {
      args.minTupleSize == 2 && args.maxTupleSize == Some(2) &&
        args.leftType(0).subsetOf(DataType.numeric) && args.leftType(1).subsetOf(DataType.numeric)
    }

    def returnType(args: AlgebraOp) = Set(FloatType)
  })
}
