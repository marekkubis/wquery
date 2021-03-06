package org.wquery.lang.operations

import org.wquery.model.DataType

trait ProvidesTypes {
  def leftType(pos: Int): Set[DataType]
  def rightType(pos: Int): Set[DataType]
}
