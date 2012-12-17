package org.wquery.engine

trait ProvidesTupleSizes {
  val minTupleSize: Int
  val maxTupleSize: Option[Int]
}
