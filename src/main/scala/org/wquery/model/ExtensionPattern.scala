package org.wquery.model

case class ExtensionPattern(pos: Int, extensions: List[Extension]) {
  val sourceType = extensions.map(extension => extension.relation.sourceType).toSet
  val destinationTypes = extensions.map(extension => extension.to.flatMap(arg => List(ArcType, extension.relation.arguments(arg))))
  val minDestinationTypesSize = destinationTypes.map(_.size).min
  val maxDestinationTypesSize = destinationTypes.map(_.size).max

  def destinationTypeAt(pos: Int) = {
    destinationTypes.filter(_.isDefinedAt(pos)).map(types => types(pos)).toSet
  }
}

case class Extension(relation: Relation, from: String, to: List[String])