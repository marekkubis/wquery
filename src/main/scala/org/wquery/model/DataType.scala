package org.wquery.model

sealed abstract class DataType {
  def associatedClass: java.lang.Class[_]
}

case object ArcType extends DataType {
  def associatedClass = classOf[Arc]
}

sealed abstract class NodeType extends DataType

case object SynsetType extends NodeType {
  def associatedClass = classOf[Synset]
}

case object SenseType extends NodeType {
  def associatedClass = classOf[Sense]
}

case object StringType extends NodeType {
  def associatedClass = classOf[String]
}

case object IntegerType extends NodeType {
  def associatedClass = classOf[Int]
}

case object FloatType extends NodeType {
  def associatedClass = classOf[Double]
}

case object BooleanType extends NodeType {
  def associatedClass = classOf[Boolean]
}

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

  def all = Set[DataType](SynsetType, SenseType, StringType, IntegerType, FloatType, BooleanType, ArcType)

  def numeric = Set[Set[DataType]](Set(IntegerType), Set(FloatType), Set(IntegerType, FloatType))
}
