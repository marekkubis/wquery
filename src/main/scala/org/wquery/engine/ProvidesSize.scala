package org.wquery.engine

import org.wquery.model.WordNetSchema
import org.wquery.utils.BigIntOptionW._

trait ProvidesSize { this: ProvidesTupleSizes =>
  def maxCount(wordNet: WordNetSchema): Option[BigInt]
  def maxSize(wordNet: WordNetSchema) = maxCount(wordNet) * maxTupleSize.map(BigInt(_))
}