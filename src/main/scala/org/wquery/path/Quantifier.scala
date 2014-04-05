package org.wquery.path

import scalaz._
import Scalaz._

case class Quantifier(lowerBound: Int, upperBound: Option[Int]) {
  override def toString = "{" + lowerBound + upperBound.some(ub => if (lowerBound == ub) "}" else "," + ub  + "}").none(",}")
}
