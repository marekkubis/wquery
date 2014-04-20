package org.wquery.model

import scalaz._
import Scalaz._

case class ArcPattern(relation: Option[Relation], source: ArcPatternArgument, destination: ArcPatternArgument) {
  override def toString = (source::relation.some(_.name).none(Relation.AnyName)::destination::Nil).mkString("^")
}

case class ArcPatternArgument(name: String, nodeType: Option[NodeType]) {
  implicit val ArcPatternArgumentEqual = equalA[ArcPatternArgument]

  override def toString = name + ~nodeType.map("&" + _)
}