package org.wquery.model

import scalaz._
import Scalaz._

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destinations: List[ArcPatternArgument]) {
  override def toString = (source::relation.map(_.name).getOrElse(Relation.AnyName)::destinations).mkString("^")
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  implicit val ArcPatternArgumentEqual = equalA[ArcPatternArgument]

  override def toString = name + ~nodeType.map("&" + _)
}

object ArcPatternArgument {
  val AnyName = "_"

  val Any = ArcPatternArgument(AnyName, None)

  def anyFor(nodeType: Option[NodeType]) = ArcPatternArgument(AnyName, nodeType)
}