package org.wquery.model

sealed abstract class FunctionArgumentType

case object TupleType extends FunctionArgumentType
case class ValueType(value: BasicType) extends FunctionArgumentType

sealed abstract class Function(val name: String, val args: List[FunctionArgumentType], val result: FunctionArgumentType)

class ScalarFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType) extends Function(name, args, result)
class AggregateFunction(name: String, args: List[FunctionArgumentType], result: FunctionArgumentType) extends Function(name, args, result)
