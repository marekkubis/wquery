package org.wquery.model

class Relation(val name: String, val argumentTypes: Map[String, BasicDataType]) {

  def sourceType = argumentTypes(Relation.Source) 
  
  def destinationType = argumentTypes(Relation.Destination)

  if (!argumentTypes.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without source argument")
  
  if (!argumentTypes.contains(Relation.Destination))
    throw new IllegalArgumentException("An attempt to create Relation '" + name + "' without destination argument")
}

object Relation {
  val Source = "source" // default argument name
  val Destination = "destination" // default argument name

  def apply(name: String, stype: BasicDataType, dtype: BasicDataType): Relation
    = apply(name, stype, dtype, Map())     
  
  def apply(name: String, stype: BasicDataType, dtype: BasicDataType, atypes: Map[String, BasicDataType]): Relation
    = new Relation(name, atypes + Pair(Source, stype) + Pair(Destination, dtype))  
}
