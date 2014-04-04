package org.wquery.engine

import org.wquery.model.WordNet
import org.wquery.utils.BigIntOptionW._

trait ProvidesSize { this: ProvidesTupleSizes =>
  def maxSize(wordNet: WordNet#Schema) = maxTupleSize.map(BigInt(_))
}
