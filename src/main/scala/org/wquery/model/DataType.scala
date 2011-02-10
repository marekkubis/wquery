package org.wquery.model

sealed abstract class DataType

sealed abstract class BasicType extends DataType

case object ArcType extends BasicType

sealed abstract class NodeType extends BasicType

case object SynsetType extends NodeType
case object SenseType extends NodeType
case object StringType extends NodeType
case object IntegerType extends NodeType
case object FloatType extends NodeType
case object BooleanType extends NodeType

case class UnionType(types: Set[BasicType]) extends DataType

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
    case _:Arc =>
      ArcType
    case obj =>
      throw new IllegalArgumentException("Object " + obj + " has no data type bound")
  }     
}