package org.wquery.model

case class ExtensionPattern(val pos: Int, val extensions: List[Extension]) {
  val destinationTypes = extensions.map(extension => extension.to.flatMap(arg => List(ArcType, extension.relation.arguments(arg))))
}

case class Extension(val relation: Relation, val from: String, val to: List[String])