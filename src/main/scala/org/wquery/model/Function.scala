package org.wquery.model

sealed abstract class FunctionArgumentType

case object TupleType extends FunctionArgumentType
case class ValueType(value: BasicDataType) extends FunctionArgumentType

sealed abstract class Function(n: String, a: List[FunctionArgumentType], r: FunctionArgumentType) {  
  def name = n
  def args = a
  def result = r
}

class ScalarFunction(n: String, a: List[FunctionArgumentType], r: FunctionArgumentType) extends Function(n, a, r)
class AggregateFunction(n: String, a: List[FunctionArgumentType], r: FunctionArgumentType) extends Function(n, a, r)
