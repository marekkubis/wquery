package org.wquery.model

class Relation(nm: String, args: Map[String, DataType]) {
  val name = nm
  val argumentsTypes = args  

  def sourceType = args(Relation.Source) 
  
  def destinationType = args(Relation.Destination)

  if (!args.contains(Relation.Source))
    throw new IllegalArgumentException("An attempt to create relation '" + nm + "' without source argument")
  
  if (!args.contains(Relation.Destination))
    throw new IllegalArgumentException("An attempt to create relation '" + nm + "' without destination argument")
}

object Relation {
  val Source = "source" // default argument name
  val Destination = "destination" // default argument name

  def apply(name: String, stype: DataType, dtype: DataType): Relation
    = apply(name, stype, dtype, Map())     
  
  def apply(name: String, stype: DataType, dtype: DataType, atypes: Map[String, DataType]): Relation
    = new Relation(name, atypes + Pair(Source, stype) + Pair(Destination, dtype))  
}
