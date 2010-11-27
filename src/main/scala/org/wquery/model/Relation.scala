package org.wquery.model

class Relation(nm: String, args: Map[String, BasicDataType]) {
  val name = nm
  val argumentsTypes = args  

  def sourceType = args(Relation.Source) 
  
  def destinationType = args(Relation.Destination)

  if (!args.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create Relation '" + nm + "' without source argument")
  
  if (!args.contains(Relation.Destination))
    throw new IllegalArgumentException("An attempt to create Relation '" + nm + "' without destination argument")
}

object Relation {
  val Source = "source" // default argument name
  val Destination = "destination" // default argument name

  def apply(name: String, stype: BasicDataType, dtype: BasicDataType): Relation
    = apply(name, stype, dtype, Map())     
  
  def apply(name: String, stype: BasicDataType, dtype: BasicDataType, atypes: Map[String, BasicDataType]): Relation
    = new Relation(name, atypes + Pair(Source, stype) + Pair(Destination, dtype))  
}
