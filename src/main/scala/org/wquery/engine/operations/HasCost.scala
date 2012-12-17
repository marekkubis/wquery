package org.wquery.engine.operations

import org.wquery.model.WordNetSchema

trait HasCost {
  def cost(wordNet: WordNetSchema): Option[BigInt]
}
