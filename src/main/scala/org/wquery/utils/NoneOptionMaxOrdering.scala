package org.wquery.utils

class NoneOptionMaxOrdering[A](implicit valueOrdering: Ordering[A]) extends Ordering[Option[A]] {
  def compare(x: Option[A], y: Option[A]) = {
    x.map(xValue => y.map(yValue => valueOrdering.compare(xValue, yValue)).getOrElse(-1)).getOrElse(1)
  }
}
