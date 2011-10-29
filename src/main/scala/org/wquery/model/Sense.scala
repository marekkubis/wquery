package org.wquery.model

case class Sense(wordForm: String, senseNumber: Int, pos: String) {
  override def toString = wordForm + ":" + senseNumber + ":" + pos
}
