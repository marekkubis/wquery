package org.wquery.engine.operations

import collection.mutable.ListBuffer
import java.lang.reflect.Method
import scalaz._
import Scalaz._
import org.wquery.model._
import org.wquery.utils.BigIntOptionW._

abstract class Function(val name: String) {
  def accepts(args: AlgebraOp): Boolean
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(args: AlgebraOp, pos: Int): Set[DataType]
  def rightType(args: AlgebraOp, pos: Int): Set[DataType]
  def minTupleSize(args: AlgebraOp): Int
  def maxTupleSize(args: AlgebraOp): Option[Int]
  def bindingsPattern(args: AlgebraOp): BindingsPattern
  def maxCount(args: AlgebraOp, wordNet: WordNetSchema): Option[BigInt]
  def cost(args: AlgebraOp, wordNet: WordNetSchema): Option[BigInt]
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

trait CountProportionalCost { this: Function =>
  def cost(args: AlgebraOp, wordNet: WordNetSchema) = maxCount(args, wordNet)
}

trait PreservesTupleSizes {
  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object ProdFunction extends DataSetFunction("prod") with AcceptsAll with ReturnsDataSetOfSimilarSize
with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int]
      val counters = Array.fill(argNum)(0)
      val lastIndex = dataSet.pathCount - 1
      val paths = dataSet.paths.map(_.init)
      val pathBuffer = DataSetBuffers.createPathBuffer
      var stop = false

      do {
        val tupleBuffer = new ListBuffer[Any]

        for (i <- 0 until argNum) {
          tupleBuffer.appendAll(paths(counters(i)))
        }

        pathBuffer.append(tupleBuffer.toList)

        val incIndex = counters.lastIndexWhere(_ < lastIndex)

        if (incIndex != -1)
          counters(incIndex) += 1
        else
          stop = true
      } while (!stop)

      DataSet(pathBuffer.toList)
    } else {
      dataSet
    }
  }

  def leftType(args: AlgebraOp, pos: Int) = {
    (for (i <- args.minTupleSize to args.maxTupleSize.getOrElse(pos + 1))
    yield args.leftType(if (i > 0) pos % i else 0)).flatten.toSet
  }

  def rightType(args: AlgebraOp, pos: Int) = {
    (for (i <- args.minTupleSize to args.maxTupleSize.getOrElse(pos + 1))
    yield args.rightType(if (i > 0) pos % i else 0)).flatten.toSet
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize - 1

  def maxTupleSize(args: AlgebraOp) = None

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object DistinctFunction extends DataSetFunction("distinct") with AcceptsAll
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern with ReturnsDataSetOfSimilarSize {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    dataSet.distinct
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object FlattenFunction extends DataSetFunction("flatten") with AcceptsAll with ReturnsValueSet with ClearsBindingsPattern {
  def returnType(args: AlgebraOp) = {
    args.maxTupleSize
      .some(size => (for (i <- 0 until size) yield args.leftType(i)).flatten.toSet)
      .none(DataType.all)
  }

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.flatten.map(x => List(x)))
  }

  def maxCount(args: AlgebraOp, wordNet: WordNetSchema) = (args.maxTupleSize <|*|> args.maxCount(wordNet)).map{ case (l, r) => l * r }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.cost(wordNet)
}

object SortFunction extends DataSetFunction("sort") with AcceptsAll with ReturnsDataSetOfSimilarSize
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.sortBy(x => x._1)(WQueryListOrdering))
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object MinFunction extends DataSetFunction("min") with AcceptsAll with ReturnsSingleTuple
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1)(WQueryListOrdering)
      val minValue = sorted.head._1

      var count = 0

      while (count < sorted.size && sorted(count)._1 == minValue) {
        count += 1
      }

      DataSet.fromBoundPaths(sorted.take(count))
    } else {
      dataSet
    }
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object MaxFunction extends DataSetFunction("max") with AcceptsAll with ReturnsSingleTuple
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1)(WQueryListOrdering)
      val maxValue = sorted.last._1

      var count = sorted.size - 2

      while (count >= 0 && sorted(count)._1 == maxValue) {
        count -= 1
      }

      DataSet.fromBoundPaths(sorted.drop(count + 1))
    } else {
      dataSet
    }
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

abstract class ByFunction(name: String) extends DataSetFunction(name) with ReturnsDataSetOfSimilarSize {
  def accepts(args: AlgebraOp) = args.minTupleSize > 0 && args.rightType(0) == Set(IntegerType)

  def leftType(args: AlgebraOp, pos: Int) = {
    if (args.maxTupleSize.some(pos < _ - 1 ).none(true))
      args.leftType(pos)
    else
      Set.empty
  }

  def rightType(args: AlgebraOp, pos: Int) = {
    if (args.maxTupleSize.some(pos < _ - 1 ).none(true))
      args.rightType(pos)
    else
      Set.empty
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize - 1

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize.map(_ - 1)

  def bindingsPattern(args: AlgebraOp) = args.bindingsPattern
}

object SortByFunction extends ByFunction("sortby") with ReturnsDataSetOfSimilarSize {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(WQueryOrdering)

      DataSet.fromBoundPaths(sorted.map{ case (x, y, z) => (x.dropRight(1), y, z) })
    } else {
      dataSet
    }
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object MinByFunction extends ByFunction("minby") with ReturnsDataSetOfSimilarSize {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(WQueryOrdering)
      val minValue = sorted.head._1(argNum)

      var count = 0

      while (count < sorted.size && sorted(count)._1(argNum) == minValue) {
        count += 1
      }

      DataSet.fromBoundPaths(sorted.take(count).map{ case (x, y, z) => (x.dropRight(1), y, z) })
    } else {
      dataSet
    }
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object MaxByFunction extends ByFunction("maxby") with ReturnsDataSetOfSimilarSize {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(WQueryOrdering)
      val maxValue = sorted.last._1(argNum)

      var count = sorted.size - 2

      while (count >= 0 && sorted(count)._1(argNum) == maxValue) {
        count -= 1
      }

      DataSet.fromBoundPaths(sorted.drop(count + 1).map{ case (x, y, z) => (x.dropRight(1), y, z) })
    } else {
      dataSet
    }
  }

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.maxCount(wordNet).map(cost => cost*cost)
}

object CountFunction extends DataSetFunction("count") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern with CountProportionalCost {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.paths.size)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LastFunction extends DataSetFunction("last") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern with CountProportionalCost {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(x => List(x.last)))
  }

  def returnType(args: AlgebraOp) = args.rightType(0)
}

object IntegerSumFunction extends DataSetFunction("sum") with AcceptsTypes with ReturnsSingleValue
 with ClearsBindingsPattern with CountProportionalCost {

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
 with ClearsBindingsPattern with CountProportionalCost {

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
 with ClearsBindingsPattern with CountProportionalCost {
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

object ShortestFunction extends DataSetFunction("shortest") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.minTupleSize))
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = Some(args.minTupleSize)

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.cost(wordNet) * some(2)
}

object LongestFunction extends DataSetFunction("longest") with AcceptsAll with ReturnsSingleTuple
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.maxTupleSize.get))
  }

  def minTupleSize(args: AlgebraOp) = args.maxTupleSize.getOrElse(args.minTupleSize)

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize

  def cost(args: AlgebraOp, wordNet: WordNetSchema) = args.cost(wordNet) * some(2)
}

object SizeFunction extends DataSetFunction("size") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern with CountProportionalCost {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LengthFunction extends DataSetFunction("length") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern with CountProportionalCost {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map{path => List(path.size)})
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object EmptyFunction extends DataSetFunction("empty") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern with CountProportionalCost {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.isEmpty)
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object StringLengthFunction extends DataSetFunction("string_length") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern with CountProportionalCost {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[String].size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object StringSplitFunction extends DataSetFunction("string_split") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern with CountProportionalCost {

  def argumentTypes = List(Set(StringType), Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, delimiter: String) =>
        word.split(delimiter).toList
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object SubstringFromFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern with CountProportionalCost {

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
 with ClearsBindingsPattern with CountProportionalCost {

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
 with ClearsBindingsPattern with CountProportionalCost {

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
 with ClearsBindingsPattern with CountProportionalCost {
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
 with ClearsBindingsPattern with CountProportionalCost {
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
 with ClearsBindingsPattern with CountProportionalCost {
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
with ClearsBindingsPattern with CountProportionalCost {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.collect{
      case List(value: Double) => List(value.toInt)
      case path @ List(value: Int) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object FloatFunction extends DataSetFunction("float") with AcceptsAll with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern with CountProportionalCost {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.collect{
      case List(value: Int) => List(value.toDouble)
      case path @ List(value: Double) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

abstract class JavaMethod(override val name: String, method: Method) extends Function(name)
 with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern with CountProportionalCost {
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

  registerFunction(ProdFunction)
  registerFunction(DistinctFunction)
  registerFunction(FlattenFunction)
  registerFunction(SortFunction)
  registerFunction(MinFunction)
  registerFunction(MaxFunction)
  registerFunction(SortByFunction)
  registerFunction(MinByFunction)
  registerFunction(MaxByFunction)
  registerFunction(CountFunction)
  registerFunction(LastFunction)
  registerFunction(IntegerSumFunction)
  registerFunction(FloatSumFunction)
  registerFunction(AvgFunction)
  registerFunction(ShortestFunction)
  registerFunction(LongestFunction)
  registerFunction(SizeFunction)
  registerFunction(LengthFunction)
  registerFunction(EmptyFunction)
  registerFunction(StringLengthFunction)
  registerFunction(StringSplitFunction)
  registerFunction(SubstringFromFunction)
  registerFunction(SubstringFromToFunction)
  registerFunction(ReplaceFunction)
  registerFunction(LowerFunction)
  registerFunction(UpperFunction)
  registerFunction(RangeFunction)
  registerFunction(IntFunction)
  registerFunction(FloatFunction)

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", IntegerType.associatedClass)) with AcceptsTypes {
    def argumentTypes = List(Set(IntegerType))
    def returnType(args: AlgebraOp) = Set(IntegerType)
  })

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("ceil", classOf[Math].getMethod("ceil", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("floor", classOf[Math].getMethod("floor", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("log", classOf[Math].getMethod("log", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("power", classOf[Math].getMethod("pow", FloatType.associatedClass, FloatType.associatedClass)) {
    def accepts(args: AlgebraOp) = {
      args.minTupleSize == 2 && args.maxTupleSize == Some(2) &&
        args.leftType(0).subsetOf(DataType.numeric) && args.leftType(1).subsetOf(DataType.numeric)
    }

    def returnType(args: AlgebraOp) = Set(FloatType)
  })
}
