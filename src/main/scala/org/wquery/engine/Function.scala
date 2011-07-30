package org.wquery.engine

import collection.mutable.ListBuffer
import java.lang.reflect.Method
import org.wquery.model._

abstract class Function(val name: String) {
  def accepts(args: AlgebraOp): Boolean
  def evaluate(args: AlgebraOp, wordNet: WordNet, bindings: Bindings): DataSet
  def leftType(args: AlgebraOp, pos: Int): Set[DataType]
  def rightType(args: AlgebraOp, pos: Int): Set[DataType]
  def minTupleSize(args: AlgebraOp): Int
  def maxTupleSize(args: AlgebraOp): Option[Int]
  def bindingsSchema(args: AlgebraOp): BindingsSchema
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
  def accepts(args: AlgebraOp) = args.minTupleSize == 1 && args.maxTupleSize == Some(1) && DataType.numeric.contains(args.rightType(0))
}

trait AcceptsTypes {
  def argumentTypes: List[Set[DataType]]

  def accepts(args: AlgebraOp) = {
    args.minTupleSize == argumentTypes.size && args.maxTupleSize == Some(argumentTypes.size) &&
      argumentTypes.zipWithIndex.forall{ case (dataType, pos) => args.leftType(pos) == dataType }
  }
}

trait PreservesBindingsSchema {
  def bindingsSchema(args: AlgebraOp) = args.bindingsSchema
}

trait ClearsBindingsSchema {
  def bindingsSchema(args: AlgebraOp) = BindingsSchema()
}

trait ReturnsSingletonTuples {
  def returnType(args: AlgebraOp): Set[DataType]

  def minTupleSize(args: AlgebraOp) = 1

  def maxTupleSize(args: AlgebraOp) = Some(1)

  def leftType(args: AlgebraOp, pos: Int): Set[DataType] = if (pos == 0) returnType(args) else Set.empty

  def rightType(args: AlgebraOp, pos: Int): Set[DataType] = if (pos == 0) returnType(args) else Set.empty
}

trait PreservesTypes {
  def leftType(args: AlgebraOp, pos: Int) = args.leftType(pos)

  def rightType(args: AlgebraOp, pos: Int) = args.rightType(pos)
}

trait PreservesSizes {
  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object DistinctFunction extends DataSetFunction("distinct") with AcceptsAll with PreservesTypes
 with PreservesSizes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.distinct)
  }
}

object SortFunction extends DataSetFunction("sort") with AcceptsAll with PreservesTypes
 with PreservesSizes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.sortBy(x => x._1)(WQueryListOrdering))
  }
}

object CountFunction extends DataSetFunction("count") with AcceptsAll with ReturnsSingletonTuples
 with ClearsBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.paths.size)
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LastFunction extends DataSetFunction("last") with AcceptsAll
 with ReturnsSingletonTuples with ClearsBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(x => List(x.last)))
  }

  def returnType(args: AlgebraOp) = args.rightType(0)
}

object IntegerSumFunction extends DataSetFunction("sum") with AcceptsTypes
 with ReturnsSingletonTuples with ClearsBindingsSchema {

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

object FloatSumFunction extends DataSetFunction("sum") with AcceptsNumbers
 with ReturnsSingletonTuples with ClearsBindingsSchema {

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

object AvgFunction extends Function("avg") with AcceptsNumbers with ReturnsSingletonTuples with ClearsBindingsSchema {
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

object MinFunction extends DataSetFunction("min") with AcceptsAll with PreservesTypes
 with PreservesSizes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
   SortFunction.evaluate(dataSet, wordNet, bindings)
     .paths.headOption.map(x => DataSet(List(x))).getOrElse(DataSet.empty)
  }
}

object MaxFunction extends DataSetFunction("max") with AcceptsAll with PreservesTypes
 with PreservesSizes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
   SortFunction.evaluate(dataSet, wordNet, bindings)
     .paths.lastOption.map(x => DataSet(List(x))).getOrElse(DataSet.empty)
  }
}

object ShortestFunction extends DataSetFunction("shortest") with AcceptsAll
 with PreservesTypes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.minTupleSize))
  }

  def minTupleSize(args: AlgebraOp) = args.minTupleSize

  def maxTupleSize(args: AlgebraOp) = Some(args.minTupleSize)
}

object LongestFunction extends DataSetFunction("longest") with AcceptsAll
 with PreservesTypes with PreservesBindingsSchema {

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromBoundPaths(dataSet.toBoundPaths.filter(p => p._1.size == dataSet.maxTupleSize))
  }

  def minTupleSize(args: AlgebraOp) = args.maxTupleSize.getOrElse(args.minTupleSize)

  def maxTupleSize(args: AlgebraOp) = args.maxTupleSize
}

object SizeFunction extends DataSetFunction("size") with AcceptsAll with ReturnsSingletonTuples with ClearsBindingsSchema {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(path => path.filter(step => !step.isInstanceOf[Arc])).map(path => List(path.size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object LengthFunction extends DataSetFunction("length") with AcceptsAll with ReturnsSingletonTuples with ClearsBindingsSchema {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map{path => List(path.size)})
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object EmptyFunction extends DataSetFunction("empty") with AcceptsAll with ReturnsSingletonTuples with ClearsBindingsSchema {
  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet.fromValue(dataSet.isEmpty)
  }

  def returnType(args: AlgebraOp) = Set(BooleanType)
}

object StringLengthFunction extends DataSetFunction("string_length") with AcceptsTypes with ReturnsSingletonTuples with ClearsBindingsSchema {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => List(tuple.last.asInstanceOf[String].size)))
  }

  def returnType(args: AlgebraOp) = Set(IntegerType)
}

object SubstringFromFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsSingletonTuples
 with ClearsBindingsSchema {

  def argumentTypes = List(Set(StringType), Set(IntegerType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, index: Int) =>
        List[Any](if (index < word.size) word.substring(index) else "")
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object SubstringFromToFunction extends DataSetFunction("substring") with AcceptsTypes with ReturnsSingletonTuples
 with ClearsBindingsSchema {

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

object ReplaceFunction extends DataSetFunction("replace") with AcceptsTypes with ReturnsSingletonTuples
 with ClearsBindingsSchema {

  def argumentTypes = List(Set(StringType), Set(StringType), Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String, pattern: String, replacement: String) =>
        List[Any](word.replaceFirst(pattern, replacement))
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object LowerFunction extends DataSetFunction("lower") with AcceptsTypes with ReturnsSingletonTuples with ClearsBindingsSchema {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String) =>
        List[Any](word.toLowerCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
}

object UpperFunction extends DataSetFunction("upper") with AcceptsTypes with ReturnsSingletonTuples with ClearsBindingsSchema {
  def argumentTypes = List(Set(StringType))

  def evaluate(dataSet: DataSet, wordNet: WordNet, bindings: Bindings) = {
    DataSet(dataSet.paths.map(tuple => (tuple: @unchecked) match {
      case List(word: String) =>
        List[Any](word.toUpperCase)
    }))
  }

  def returnType(args: AlgebraOp) = Set(StringType)
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

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", IntegerType.associatedClass))
   with AcceptsTypes with ReturnsSingletonTuples with ClearsBindingsSchema {
    def argumentTypes = List(Set(IntegerType))
    def returnType(args: AlgebraOp) = Set(IntegerType)
  })

  registerFunction(new JavaMethod("abs", classOf[Math].getMethod("abs", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsSingletonTuples with ClearsBindingsSchema {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("ceil", classOf[Math].getMethod("ceil", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsSingletonTuples with ClearsBindingsSchema {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("floor", classOf[Math].getMethod("floor", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsSingletonTuples with ClearsBindingsSchema {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("log", classOf[Math].getMethod("log", FloatType.associatedClass))
   with AcceptsNumbers with ReturnsSingletonTuples with ClearsBindingsSchema {
    def returnType(args: AlgebraOp) = Set(FloatType)
  })

  registerFunction(new JavaMethod("power", classOf[Math].getMethod("pow", FloatType.associatedClass, FloatType.associatedClass))
   with ReturnsSingletonTuples with ClearsBindingsSchema {
    def accepts(args: AlgebraOp) = {
      args.minTupleSize == 2 && args.maxTupleSize == Some(2) &&
        DataType.numeric.contains(args.leftType(0)) && DataType.numeric.contains(args.leftType(1))
    }

    def returnType(args: AlgebraOp) = Set(FloatType)
  })
}
