package org.wquery.model

sealed abstract class DataType

sealed abstract class BasicDataType extends DataType

case object SynsetType extends BasicDataType
case object SenseType extends BasicDataType
case object StringType extends BasicDataType
case object IntegerType extends BasicDataType
case object FloatType extends BasicDataType
case object BooleanType extends BasicDataType

case class UnionType(types: Set[BasicDataType]) extends DataType

object DataType {
  def apply(value: Any) = value match {        
    case _:Synset =>
      SynsetType
    case _:Sense =>
      SenseType
    case _:String =>
      StringType
    case _:Int =>
      IntegerType
    case _:Double =>
      FloatType
    case _:Boolean =>
      BooleanType
    case obj =>
      throw new IllegalArgumentException("Object " + obj + " has no data type bound")
  }     
}