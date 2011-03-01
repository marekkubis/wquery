package org.wquery.model

sealed abstract class FunctionArgumentType

case object TupleType extends FunctionArgumentType
case class ValueType(value: BasicType) extends FunctionArgumentType

sealed abstract class Function(val name: String, val args: List[FunctionArgumentType], val result: FunctionArgumentType)

case class ScalarFunction(override val name: String, override val args: List[FunctionArgumentType], 
  override val result: FunctionArgumentType) extends Function(name, args, result)

case class AggregateFunction(override val name: String, override val args: List[FunctionArgumentType], 
  override val result: FunctionArgumentType) extends Function(name, args, result)
