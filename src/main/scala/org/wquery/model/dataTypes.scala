package org.wquery.model

import org.wquery.WQueryModelException

sealed abstract class DataType extends Ordered[DataType] {
  def rank: Int

  def associatedClass: java.lang.Class[_]

  def compare(that: DataType) = rank - that.rank
}

case object ArcType extends DataType {
  def rank = 0
  def associatedClass = classOf[Arc]
  override def toString = "arc"
}

sealed abstract class NodeType extends DataType

case object SynsetType extends NodeType {
  def rank = 1
  def associatedClass = classOf[Synset]
  override def toString = "synset"
}

case object SenseType extends NodeType {
  def rank = 2
  def associatedClass = classOf[Sense]
  override def toString = "sense"
}

case object StringType extends NodeType {
  def rank = 3
  def associatedClass = classOf[String]
  override def toString = "string"
}

case object POSType extends NodeType {
  def rank = 4
  def associatedClass = classOf[String]
  override def toString = "pos"
}

case object IntegerType extends NodeType {
  def rank = 5
  def associatedClass = classOf[Int]
  override def toString = "integer"
}

case object FloatType extends NodeType {
  def rank = 6
  def associatedClass = classOf[Double]
  override def toString = "float"
}

case object BooleanType extends NodeType {
  def rank = 7
  def associatedClass = classOf[Boolean]
  override def toString = "boolean"
}

object DataType {
  val all = NodeType.all.toSet[DataType] +  ArcType
  val numeric = Set[DataType](IntegerType, FloatType)

  def fromValue(value: Any) = value match {
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

object NodeType {
  val all = Set[NodeType](SynsetType, SenseType, StringType, POSType, IntegerType, FloatType, BooleanType)

  def fromName(name: String) = name match {
    case "synset" =>
      SynsetType
    case "sense" =>
      SenseType
    case "string" =>
      StringType
    case "integer" =>
      IntegerType
    case "float" =>
      FloatType
    case "boolean" =>
      BooleanType
    case name =>
      throw new WQueryModelException("Datatype '" + name + "' not found")
  }
}
