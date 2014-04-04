package org.wquery.model

import collection.mutable.ListBuffer

trait ExtensionSet {
  def size: Int
  def leftOrShift(pathPos: Int, pos: Int): Either[Int, Any]
  def rightOrShift(pathPos: Int): Either[Int, Any]
  def extension(pathPos: Int): (Int, List[Any])

  def extensions = (0 until size).map(extension(_))

  def left(pathPos: Int, pos: Int) = {
    leftOrShift(pathPos, pos) match {
      case Right(obj) =>
        obj
      case _ =>
        throw new IllegalArgumentException("Left reference (" + pos + ") too far for path " + pathPos)
    }
  }

  def right(pathPos: Int) = {
    rightOrShift(pathPos) match {
      case Right(obj) =>
        obj
      case _ =>
        throw new IllegalArgumentException("Right reference too far for path " + pathPos)
    }
  }
}

class DataExtensionSet(dataSet: DataSet) extends ExtensionSet {
  def size = dataSet.pathCount

  def leftOrShift(pathPos: Int, pos: Int) = {
    val path = dataSet.paths(pathPos)
    if (pos < path.size) Right(path(pos)) else Left(path.size)
  }

  def rightOrShift(pathPos: Int) = Right(dataSet.paths(pathPos).last)

  def extension(pathPos: Int) = (pathPos, Nil)
}

sealed abstract class ExtendedExtensionSet(val parent: ExtensionSet, val extensionsList: List[(Int,  List[Any])]) extends ExtensionSet {
  def size = extensionsList.size
}

class RightExtendedExtensionSet(override val parent: ExtensionSet, override val extensionsList: List[(Int,  List[Any])])
  extends ExtendedExtensionSet(parent, extensionsList) {
  def leftOrShift(pathPos: Int, pos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)

    parent.leftOrShift(parentPos, pos) match {
      case Left(shift) =>
        val index = pos - shift

        if (index < extension.size)
          Right(extension(index))
        else
          Left(shift + extension.size)
      case right =>
        right
    }
  }

  def rightOrShift(pathPos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    Right(extension.last)
  }

  def extension(pathPos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    val (parentParentPos, parentExtension) = parent.extension(parentPos)

    (parentParentPos, parentExtension ++ extension)
  }
}

class ExtensionSetBuilder(parent: ExtensionSet) {
  private val extensions = new ListBuffer[(Int, List[Any])]

  def extend(pathPos: Int, values: List[Any]) = extensions.append((pathPos, values))

  def build = new RightExtendedExtensionSet(parent, extensions.toList)
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
