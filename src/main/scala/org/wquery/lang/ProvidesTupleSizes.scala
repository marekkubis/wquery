package org.wquery.lang.operations

trait ProvidesTupleSizes {
  val minTupleSize: Int
  val maxTupleSize: Option[Int]
}
