package org.wquery.utils
import scalaz._
import Scalaz._

class IntOptionW(value: Option[Int]) {
  def +(rvalue: Option[Int]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt + rightInt }

  def *(rvalue: Option[Int]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt * rightInt }

  def -(rvalue: Int) = value.map(_ - rvalue)

  def max(rvalue: Option[Int]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt max rightInt }

  def min(rvalue: Option[Int]) = (value, rvalue) match {
    case (Some(leftInt), Some(rightInt)) =>
      Some(leftInt min rightInt)
    case (Some(leftInt), None) =>
      Some(leftInt)
    case (None, Some(rightInt)) =>
      Some(rightInt)
    case _ =>
      None
  }
}

object IntOptionW {
  implicit def intOptionToIntOptionW(value: Option[Int]) = new IntOptionW(value)
}
