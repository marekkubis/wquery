package org.wquery.engine.operations

import org.wquery.model.WordNet

trait HasCost {
  def cost(wordNet: WordNet#Schema): Option[BigInt]
}
