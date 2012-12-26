package org.wquery.engine

import org.wquery.model.WordNet
import org.wquery.utils.BigIntOptionW._

trait ProvidesSize { this: ProvidesTupleSizes =>
  def maxCount(wordNet: WordNet#Schema): Option[BigInt]
  def maxSize(wordNet: WordNet#Schema) = maxCount(wordNet) * maxTupleSize.map(BigInt(_))
}
