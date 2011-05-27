package org.wquery.model

class Synset(val id: String, val senses: List[Sense]) {  
  override def toString = "{" + senses.mkString(" ") + "}"
}
