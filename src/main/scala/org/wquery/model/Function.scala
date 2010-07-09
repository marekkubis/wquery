package org.wquery.model

sealed abstract class FunctionType

case object ScalarType extends FunctionType // transforms members of basic data type to basic data type value
case object AggregateType extends FunctionType // transforms tuples or basic data types elements to tuples

sealed abstract class FunctionArgumentType

case object TupleType extends FunctionArgumentType
case class ValueType(value: BasicDataType) extends FunctionArgumentType

class Function(n: String, a: List[FunctionArgumentType], r: FunctionArgumentType, f: FunctionType) {  
  def name = n
  def args = a
  def result = r
  def functionType = f
}
