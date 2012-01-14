package org.wquery.model

case class PropertyAssignment(pattern: ArcPattern, op: String, values: DataSet) {
  def tuplesFor(node: Any) = {
    val destinationNames = pattern.destinations.map(_.name)
    (for (args <- values.paths) yield ((pattern.source.name, node)::(destinationNames.zip(args.slice(args.size - pattern.destinations.size, args.size)))).toMap)
  }
}



