package org.wquery.model

import collection.mutable.ListBuffer

trait ExtensionSet {
  def size: Int
  def right(pathPos: Int, pos: Int): Any

  def extension(pathPos: Int): List[Any]
  def extensions = (0 until size).map(extension(_))
}

class DataExtensionSet(dataSet: DataSet) extends ExtensionSet {
  def size = dataSet.pathCount

  def right(pathPos: Int, pos: Int) = {
    val path = dataSet.paths(pathPos)
    path(path.size - 1 - pos)
  }

  def extension(pathPos: Int) = List(pathPos)
}

class ExtendedExtensionSet(val parent: ExtensionSet, val extensionsList: List[(Int,  List[Any])]) extends ExtensionSet {
  def size = extensionsList.size

  def right(pathPos: Int, pos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    
    if (pos < extension.size)
      extension(extension.size - 1 - pos)
    else
      parent.right(parentPos, pos - extension.size)
  }

  def extension(pathPos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    parent.extension(parentPos) ++ extension
  }
}

class ExtensionSetBuilder(parent: ExtensionSet) {
  private val extensions = new ListBuffer[(Int, List[Any])]

  def extend(pathPos: Int, values: List[Any]) = extensions.append((pathPos, values))

  def build = new ExtendedExtensionSet(parent, extensions.toList)
}

class ExtensionSetBuffer(parent: ExtensionSet) {
  private val extensionSets = new ListBuffer[ExtendedExtensionSet]

  def append(extensionSet: ExtensionSet) = extensionSet match {
    case extensionSet: ExtendedExtensionSet =>
      if (extensionSet.parent == parent)
        extensionSets.append(extensionSet)
      else
        throw new IllegalArgumentException("ExtensionSetBuffer: extensionSet.parent != parent")
    case _ =>
      throw new IllegalArgumentException("ExtensionSetBuffer: extensionSet has no parent")
  }

  def toExtensionSet = {
    val builder = new ExtensionSetBuilder(parent)

    for (extensionSet <- extensionSets; (pathPos, extension) <- extensionSet.extensionsList)
      builder.extend(pathPos, extension)

    builder.build
  }
}
