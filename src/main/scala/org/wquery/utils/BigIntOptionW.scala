package org.wquery.utils
import scalaz._
import Scalaz._

class BigIntOptionW(value: Option[BigInt]) extends Ordered[Option[BigInt]] {
  def +(rvalue: Option[BigInt]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt + rightInt }

  def *(rvalue: Option[BigInt]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt * rightInt }

  def max(rvalue: Option[BigInt]) = (value <|*|> rvalue).map{ case (leftInt, rightInt) => leftInt max rightInt }

  def min(rvalue: Option[BigInt]) = (value, rvalue) match {
    case (Some(leftInt), Some(rightInt)) =>
      Some(leftInt min rightInt)
    case (Some(leftInt), None) =>
      Some(leftInt)
    case (None, Some(rightInt)) =>
      Some(rightInt)
    case _ =>
      None
  }

  def compare(rvalue: Option[BigInt]) = BigIntOptionW.NoneMaxOrdering.compare(value, rvalue)
}

object BigIntOptionW {
  implicit def bigIntOptionToBigIntOptionW(value: Option[BigInt]) = new BigIntOptionW(value)

  implicit val NoneMaxOrdering = new NoneOptionMaxOrdering[BigInt]()
}
