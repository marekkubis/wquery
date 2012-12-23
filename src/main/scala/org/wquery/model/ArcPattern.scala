package org.wquery.model

import scalaz._
import Scalaz._

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destinations: List[ArcPatternArgument]) {
  override def toString = (source::relation.some(_.name).none(Relation.AnyName)::destinations).mkString("^")
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
