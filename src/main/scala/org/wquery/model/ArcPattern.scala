package org.wquery.model

import scalaz.Scalaz._

case class ArcPattern(relations: Option[List[Relation]], source: ArcPatternArgument, destination: ArcPatternArgument) {
  override def toString = (source::relations.some(_.head.name).none(Relation.AnyName)::destination::Nil).mkString("^")
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  implicit val ArcPatternArgumentEqual = equalA[ArcPatternArgument]

  def isUnnamed = name == ArcPatternArgument.AnyName

  override def toString = name + ~nodeType.map("&" + _)
}

object ArcPatternArgument {
  val AnyName = "_"

  val Any = ArcPatternArgument(AnyName, None)

  def anyFor(nodeType: Option[NodeType]) = ArcPatternArgument(AnyName, nodeType)
}
