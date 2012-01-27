package org.wquery.engine.operations

trait ProvidesTupleSizes {
  val minTupleSize: Int
  val maxTupleSize: Option[Int]
}