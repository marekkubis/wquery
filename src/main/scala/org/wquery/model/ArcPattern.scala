package org.wquery.model

import scalaz._
import Scalaz._

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destinations: List[ArcPatternArgument]) {
  override def toString = (source::relation.map(_.name).getOrElse("_")::destinations).mkString("^")
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  implicit val ArcPatternArgumentEqual = equalA[ArcPatternArgument]

  override def toString = name + ~nodeType.map("&" + _)
}