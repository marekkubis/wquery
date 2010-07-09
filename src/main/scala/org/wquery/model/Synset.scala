package org.wquery.model

class Synset(i: String, s: List[Sense]) {
  val id = i
  val senses = s   
  
  override def toString = "{" + senses.mkString(" ") + "}"
}
