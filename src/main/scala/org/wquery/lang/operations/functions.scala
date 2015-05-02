// scalastyle:off number.of.types

package org.wquery.lang.operations

import java.lang.reflect.Method
import java.util.IllegalFormatException

import org.wquery.FormatFunctionException
import org.wquery.lang._
import org.wquery.model._

import scala.collection.mutable.ListBuffer
import scalaz.Scalaz._

abstract class Function(val name: String) {
  def accepts(args: Option[AlgebraOp]): Boolean
  def evaluate(args: Option[AlgebraOp], wordNet: WordNet, bindings: Bindings, context: Context): DataSet
  def leftType(args: Option[AlgebraOp], pos: Int): Set[DataType]
  def rightType(args: Option[AlgebraOp], pos: Int): Set[DataType]
  def minTupleSize(args: Option[AlgebraOp]): Int
  def maxTupleSize(args: Option[AlgebraOp]): Option[Int]
  def bindingsPattern(args: Option[AlgebraOp]): BindingsPattern
  override def toString = name
}

abstract class DataSetFunction(override val name: String) extends Function(name) with HasArguments {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context): DataSet

  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings, context: Context) = {
    evaluate(args.evaluate(wordNet, bindings, context), wordNet, bindings, context)
  }
}

trait HasArguments {
  def accepts(args: AlgebraOp): Boolean
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings, context: Context): DataSet
  def leftType(args: AlgebraOp, pos: Int): Set[DataType]
  def rightType(args: AlgebraOp, pos: Int): Set[DataType]
  def minTupleSize(args: AlgebraOp): Int
  def maxTupleSize(args: AlgebraOp): Option[Int]
  def bindingsPattern(args: AlgebraOp): BindingsPattern

  def accepts(args: Option[AlgebraOp]): Boolean = accepts(args.get)

  def evaluate(args: Option[AlgebraOp], wordNet: WordNet, bindings: Bindings, context: Context): DataSet = {
    evaluate(args.get, wordNet, bindings, context)
  }

  def leftType(args: Option[AlgebraOp], pos: Int): Set[DataType] = leftType(args.get, pos)

  def rightType(args: Option[AlgebraOp], pos: Int): Set[DataType] = rightType(args.get, pos)

  def minTupleSize(args: Option[AlgebraOp]): Int = minTupleSize(args.get)

  def maxTupleSize(args: Option[AlgebraOp]): Option[Int] = maxTupleSize(args.get)

  def bindingsPattern(args: Option[AlgebraOp]): BindingsPattern = bindingsPattern(args.get)
}

trait HasNoArguments {
  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context): DataSet

  def accepts(args: Option[AlgebraOp]) = args.isEmpty
  def evaluate(args: Option[AlgebraOp], wordNet: WordNet, bindings: Bindings, context: Context): DataSet = {
    evaluate(wordNet, bindings, context)
  }
  def leftType(args: Option[AlgebraOp], pos: Int) = DataType.empty
  def rightType(args: Option[AlgebraOp], pos: Int) = DataType.empty
  def minTupleSize(args: Option[AlgebraOp]) = 0
  def maxTupleSize(args: Option[AlgebraOp]) = Some(0)
  def bindingsPattern(args: Option[AlgebraOp]) = BindingsPattern()
}

trait AcceptsAll extends HasArguments {
  def accepts(args: AlgebraOp) = true
}

trait AcceptsNumbers extends HasArguments {
  def accepts(args: AlgebraOp) = args.rightType(0).subsetOf(DataType.numeric)
}

trait AcceptsTypes extends HasArguments {
  def argumentTypes: List[Set[DataType]]

  def accepts(args: AlgebraOp) = {
    args.minTupleSize >= argumentTypes.size &&
      argumentTypes.zipWithIndex.forall{ case (dataType, pos) => args.leftType(pos).subsetOf(dataType) }
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

trait ReturnsSingleValue extends ReturnsValueSet

trait ReturnsValueSetOfSimilarSize extends ReturnsValueSet

trait PreservesTypes {
  def leftType(args: AlgebraOp, pos: Int) = args.leftType(pos)

  def rightType(args: AlgebraOp, pos: Int) = args.rightType(pos)
}

trait PreservesTupleSizes {
  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object ProdFunction extends DataSetFunction("prod") with AcceptsAll 
with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
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
    (for (i <- args.minTupleSize to args.maxTupleSize|(pos + 1))
    yield args.leftType(if (i > 0) pos % i else 0)).flatten.toSet
  }

  def rightType(args: AlgebraOp, pos: Int) = {
    (for (i <- args.minTupleSize to args.maxTupleSize|(pos + 1))
    yield args.rightType(if (i > 0) pos % i else 0)).flatten.toSet
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize - 1

  def maxTupleSize(args: AlgebraOp) = None

}

object DistinctFunction extends DataSetFunction("distinct") with AcceptsAll
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    dataSet.distinct
  }

}

object FlattenFunction extends DataSetFunction("flatten") with AcceptsAll with ReturnsValueSet with ClearsBindingsPattern {
  def returnType(args: AlgebraOp) = {
    args.maxTupleSize
      .some(size => (for (i <- 0 until size) yield args.leftType(i)).flatten.toSet)
      .none(DataType.all)
  }

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.flatten.map(x => List(x)))
  }

}

object SortFunction extends DataSetFunction("sort") with AcceptsAll 
 with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.sortBy(x => x._1)(AnyListOrdering))
  }

}

object MinFunction extends DataSetFunction("min") with AcceptsAll
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (!dataSet.isEmpty) {
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1)(AnyListOrdering)
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

}

object MaxFunction extends DataSetFunction("max") with AcceptsAll
with PreservesTypes with PreservesTupleSizes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (!dataSet.isEmpty) {
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1)(AnyListOrdering)
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

}

abstract class ByFunction(name: String) extends DataSetFunction(name) {
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

object SortByFunction extends ByFunction("sortby") {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(AnyOrdering)

      DataSet.fromBoundPaths(sorted.map{ case (x, y, z) => (x.dropRight(1), y, z) })
    } else {
      dataSet
    }
  }

}

object MinByFunction extends ByFunction("minby") {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(AnyOrdering)
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

}

object MaxByFunction extends ByFunction("maxby") {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    if (!dataSet.isEmpty) {
      val argNum = dataSet.paths.head.last.asInstanceOf[Int] - 1
      val sorted = dataSet.toBoundPaths.sortBy(x => x._1(argNum))(AnyOrdering)
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

}

object CountFunction extends DataSetFunction("count") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromValue(dataSet.paths.size)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LastFunction extends DataSetFunction("last") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(x => List(x.last)))
  }

  def returnType(args: AlgebraOp) = args.rightType(0)
}

object IntegerSumFunction extends DataSetFunction("sum") with AcceptsTypes with ReturnsSingleValue
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    var sum: Int = 0

    for (tuple <- dataSet.paths) {
        sum += tuple.last.asInstanceOf[Int]
    }

    DataSet.fromValue(sum)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object FloatSumFunction extends DataSetFunction("sum") with AcceptsNumbers with ReturnsSingleValue
 with ClearsBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    var sum: Double = 0

    for (tuple <- dataSet.paths) {
      tuple.last match {
        case value:Double =>
          sum += value
        case value:Int =>
          sum += value
      }
    }

    DataSet.fromValue(sum)
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

object AvgFunction extends Function("avg") with AcceptsNumbers with ReturnsSingleValue
 with ClearsBindingsPattern {
  def evaluate(op: AlgebraOp, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val dataSet = op.evaluate(wordNet, bindings, context)

    if (op.rightType(0) == Set(IntegerType)) {
      DataSet.fromValue(IntegerSumFunction.evaluate(dataSet, wordNet, bindings, context)
        .paths.head.head.asInstanceOf[Int].toDouble / dataSet.paths.size)
    } else {
      DataSet.fromValue(FloatSumFunction.evaluate(dataSet, wordNet, bindings, context)
        .paths.head.head.asInstanceOf[Double] / dataSet.paths.size)
    }
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

object ShortestFunction extends DataSetFunction("shortest") with AcceptsAll
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.minTupleSize))
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = Some(args.minTupleSize)

}

object LongestFunction extends DataSetFunction("longest") with AcceptsAll
 with PreservesTypes with PreservesBindingsPattern {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.maxTupleSize.get))
  }

  def minTupleSize(args: AlgebraOp) = args.maxTupleSize|args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize

}

object SizeFunction extends DataSetFunction("size") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LengthFunction extends DataSetFunction("length") with AcceptsAll with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map{path => List(path.size)})
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object EmptyFunction extends DataSetFunction("empty") with AcceptsAll with ReturnsSingleValue
 with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromValue(dataSet.isEmpty)
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object ReadFunction extends Function("read") with HasNoArguments with ReturnsSingleValue
with ClearsBindingsPattern {

  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    val line = Console.readLine()

    if (line != null) {
      DataSet.fromValue(line.stripLineEnd)
    } else {
      DataSet.empty
    }
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object StringLengthFunction extends DataSetFunction("string_length") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[String].size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object StringSplitFunction extends DataSetFunction("string_split") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(StringType), Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => (tuple.takeRight(2): @unchecked) match {
      case List(word: String, delimiter: String) =>
        word.split(delimiter).toList
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object SubstringFromToFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {

  def argumentTypes = List(Set(StringType), Set(IntegerType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => (tuple.takeRight(3): @unchecked) match {
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

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => (tuple.takeRight(3): @unchecked) match {
      case List(word: String, pattern: String, replacement: String) =>
        List[Any](word.replaceFirst(pattern, replacement))
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object LowerFunction extends DataSetFunction("lower") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => tuple.last match {
      case word: String =>
        List[Any](word.toLowerCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object UpperFunction extends DataSetFunction("upper") with AcceptsTypes with ReturnsValueSetOfSimilarSize
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => tuple.last match {
      case word: String =>
        List[Any](word.toUpperCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object IsSynsetFunction extends DataSetFunction("is_synset") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Synset])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsSenseFunction extends DataSetFunction("is_sense") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Sense])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsStringFunction extends DataSetFunction("is_string") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[String])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsIntegerFunction extends DataSetFunction("is_integer") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Int])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsFloatFunction extends DataSetFunction("is_float") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Double])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsBooleanFunction extends DataSetFunction("is_boolean") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Boolean])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object IsArcFunction extends DataSetFunction("is_arc") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(DataType.all)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.isInstanceOf[Arc])))
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object RangeFunction extends DataSetFunction("range") with AcceptsTypes with ReturnsValueSet
 with ClearsBindingsPattern {
  def argumentTypes = List(Set(IntegerType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.flatMap(tuple => (tuple.takeRight(2): @unchecked) match {
      case List(left: Int, right: Int) =>
        (left to right).map(List(_))
    }))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)

}

object IntFunction extends DataSetFunction("int") with AcceptsAll with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.collect{
      case List(value: Double) => List(value.toInt)
      case path @ List(value: Int) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object FloatFunction extends DataSetFunction("float") with AcceptsAll with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.collect{
      case List(value: Int) => List(value.toDouble)
      case path @ List(value: Double) => path
    })
  }

  def returnType(args: AlgebraOp) = Set(FloatType)
}

object AsTupleFunction extends DataSetFunction("as_tuple")
with ClearsBindingsPattern {
  val DefaultSeparator = "\t"
  val tokenParsers = new Object with WTokenParsers

  override def accepts(args: AlgebraOp) = args.rightType(0) == Set(StringType)

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map{ tuple =>
      val (literal, sep) = tuple.takeRight(2) match {
        case List(left: String, right: String) =>
          if (right.startsWith("/") && right.endsWith("/")) {
            (left, right.substring(1, right.size - 1))
          } else {
            (right, DefaultSeparator)
          }
        case List(left, right: String) =>
          (right, DefaultSeparator)
        case List(elem: String) =>
          (elem, DefaultSeparator)
      }

      asTuple(wordNet, literal, sep)
    })
  }

  def asTuple(wordNet: WordNet, value: String, sep: String) = {
    value.split(sep).map { elem =>
      tokenParsers.parseAll(tokenParsers.tokenLit, elem) match {
        case tokenParsers.Success((SynsetType, (word: String, num: Int, pos: String)), _) =>
          wordNet.getSense(word, num, pos).map(wordNet.getSynset).flatten
        case tokenParsers.Success((SenseType, (word: String, num: Int, pos: String)), _) =>
          wordNet.getSense(word, num, pos)
        case tokenParsers.Success((StringType, false, value: String), _) =>
          Some(value)
        case tokenParsers.Success((StringType, true, value: String), _) =>
          wordNet.getWord(value)
        case tokenParsers.Success((FloatType, num), _) =>
          Some(num)
        case tokenParsers.Success((IntegerType, num), _) =>
          Some(num)
        case _ =>
          None
      }
    }.toList.flatten
  }

  override def minTupleSize(args: AlgebraOp) = 0

  override def maxTupleSize(args: AlgebraOp) = None

  override def leftType(args: AlgebraOp, pos: Int) = DataType.all

  override def rightType(args: AlgebraOp, pos: Int) = DataType.all

}

object ArcNameFunction extends DataSetFunction("arc_name") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(Set(ArcType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[Arc].relation.name)))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object ArcSourceFunction extends DataSetFunction("arc_src") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(Set(ArcType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[Arc].from)))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object ArcDestinationFunction extends DataSetFunction("arc_dst") with AcceptsTypes with ReturnsValueSetOfSimilarSize
with ClearsBindingsPattern {
  def argumentTypes = List(Set(ArcType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[Arc].to)))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

abstract class ForEachTupleFunction(override val name: String) extends DataSetFunction(name)
 with ClearsBindingsPattern {
  def invoke(args: List[Any]): List[Any]

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val buffer = new ListBuffer[List[Any]]()

    for (tuple <- dataSet.paths)
      buffer.append(invoke(tuple))

    DataSet(buffer.toList)
  }
}

object FormatFunction extends ForEachTupleFunction("format") with ReturnsValueSetOfSimilarSize {
  override def returnType(args: AlgebraOp) = Set(StringType)

  override def invoke(args: List[Any]) = {
    try {
      val callArgs = args.tail.map {
        case arg: Integer => arg
        case arg: Double => arg
        case arg: String => arg
        case arg: Boolean => arg
        case arg => arg.toString
      }.asInstanceOf[List[AnyRef]]

      List(String.format(args.head.asInstanceOf[String], args.tail.asInstanceOf[List[AnyRef]]: _*))
    } catch {
      case e: IllegalFormatException =>
        throw new FormatFunctionException(e.getMessage)
    }
  }

  override def accepts(args: AlgebraOp) = args.leftType(0) == Set(StringType)
}

abstract class JavaMethod(override val name: String, method: Method) extends Function(name)
 with ReturnsValueSetOfSimilarSize with ClearsBindingsPattern {
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings, context: Context) = {
    val argsValues = args.evaluate(wordNet, bindings, context)
    val buffer = new ListBuffer[List[Any]]()
    val invocationArgs = new Array[AnyRef](method.getParameterTypes.size)

    for (tuple <- argsValues.paths) {
      val offset = tuple.size - invocationArgs.size

      for (i <- 0 until invocationArgs.size)
        invocationArgs(i) = tuple(offset + i).asInstanceOf[AnyRef]

      // scalastyle:off null
      buffer.append(List(method.invoke(null, invocationArgs: _*)))
      // scalastyle:on null
    }

    DataSet(buffer.toList)
  }

  def evaluate(wordNet: WordNet, bindings: Bindings, context: Context) = {
    DataSet.fromValue(method.invoke(null))
  }
}

// scalastyle:off multiple.string.literals

object Functions {
  private val functions = scala.collection.mutable.Map[String, List[Function]]()

  def registerFunction(function: Function) {
    functions.get(function.name).some(l => functions(function.name) = (l :+ function))
      .none(functions(function.name) = List(function))
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
  registerFunction(ReadFunction)
  registerFunction(StringLengthFunction)
  registerFunction(StringSplitFunction)
  registerFunction(SubstringFromToFunction)
  registerFunction(ReplaceFunction)
  registerFunction(LowerFunction)
  registerFunction(UpperFunction)
  registerFunction(FormatFunction)
  registerFunction(IsSynsetFunction)
  registerFunction(IsSenseFunction)
  registerFunction(IsStringFunction)
  registerFunction(IsIntegerFunction)
  registerFunction(IsFloatFunction)
  registerFunction(IsBooleanFunction)
  registerFunction(IsArcFunction)
  registerFunction(RangeFunction)
  registerFunction(IntFunction)
  registerFunction(FloatFunction)
  registerFunction(AsTupleFunction)
  registerFunction(ArcNameFunction)
  registerFunction(ArcSourceFunction)
  registerFunction(ArcDestinationFunction)

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

  registerFunction(new JavaMethod("exp", classOf[Math].getMethod("exp", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("floor", classOf[Math].getMethod("floor", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("log", classOf[Math].getMethod("log", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("power", classOf[Math].getMethod("pow", FloatType.associatedClass, FloatType.associatedClass)) with HasArguments {
    def accepts(args: AlgebraOp) = {
      args.minTupleSize == 2 && args.maxTupleSize == Some(2) &&
        args.leftType(0).subsetOf(DataType.numeric) && args.leftType(1).subsetOf(DataType.numeric)
    }

    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("random", classOf[Math].getMethod("random")) with HasNoArguments {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("round", classOf[Math].getMethod("round", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(IntegerType)
  })

  registerFunction(new JavaMethod("sqrt", classOf[Math].getMethod("sqrt", FloatType.associatedClass)) with AcceptsNumbers {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })
}
