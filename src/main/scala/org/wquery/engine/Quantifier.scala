package org.wquery.engine

case class Quantifier(lowerBound: Int, upperBound: Option[Int]) {
  override def toString = "{" + lowerBound + upperBound.map(ub => if (lowerBound == ub) "}" else "," + ub  + "}").getOrElse(",}")
}
