package org.wquery.model

import collection.mutable.ListBuffer
import org.wquery.engine.{Direction, Forward, Backward, Side}
import org.wquery.WQueryEvaluationException

trait ExtensionSet {
  def size: Int
  def leftOrShift(pathPos: Int, pos: Int): Either[Int, Any]
  def rightOrShift(pathPos: Int, pos: Int): Either[Int, Any]
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

  def right(pathPos: Int, pos: Int) = {
    rightOrShift(pathPos, pos) match {
      case Right(obj) =>
        obj
      case _ =>
        throw new IllegalArgumentException("Right reference (" + pos + ") too far for path " + pathPos)
    }
  }
}

class DataExtensionSet(dataSet: DataSet) extends ExtensionSet {
  def size = dataSet.pathCount

  def leftOrShift(pathPos: Int, pos: Int) = {
    val path = dataSet.paths(pathPos)
    if (pos < path.size) Right(path(pos)) else Left(path.size)
  }

  def rightOrShift(pathPos: Int, pos: Int) = {
    val path = dataSet.paths(pathPos)
    val index = path.size - 1 - pos

    if (index < path.size) Right(path(index)) else Left(path.size)
  }

  def extension(pathPos: Int) = (pathPos, Nil)
}

sealed abstract class ExtendedExtensionSet(val parent: ExtensionSet, val extensionsList: List[(Int,  List[Any])]) extends ExtensionSet {
  def size = extensionsList.size
}

class LeftExtendedExtensionSet(override val parent: ExtensionSet, override val extensionsList: List[(Int,  List[Any])]) extends ExtendedExtensionSet(parent, extensionsList) {
  def leftOrShift(pathPos: Int, pos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)

    if (pos < extension.size)
      Right(extension(pos))
    else
      parent.leftOrShift(parentPos, pos - extension.size)
  }

  def rightOrShift(pathPos: Int, pos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)

    parent.rightOrShift(parentPos, pos) match {
      case Left(shift) =>
        val index = pos - shift

        if (index < extension.size)
          Right(extension(extension.size - 1 - index))
        else
          Left(shift + extension.size)
      case right =>
        right
    }
  }

  def extension(pathPos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    val (parentParentPos, parentExtension) = parent.extension(parentPos)

    (parentParentPos, extension ++ parentExtension)
  }
}

class RightExtendedExtensionSet(override val parent: ExtensionSet, override val extensionsList: List[(Int,  List[Any])]) extends ExtendedExtensionSet(parent, extensionsList) {
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

  def rightOrShift(pathPos: Int, pos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)

    if (pos < extension.size)
      Right(extension(extension.size - 1 - pos))
    else
      parent.rightOrShift(parentPos, pos - extension.size)
  }

  def extension(pathPos: Int) = {
    val (parentPos, extension) = extensionsList(pathPos)
    val (parentParentPos, parentExtension) = parent.extension(parentPos)

    (parentParentPos, parentExtension ++ extension)
  }
}

class ExtensionSetBuilder(parent: ExtensionSet, direction: Direction) {
  private val extensions = new ListBuffer[(Int, List[Any])]

  def extend(pathPos: Int, values: List[Any]) = extensions.append((pathPos, values))

  def build = direction match {
    case Forward =>
      new RightExtendedExtensionSet(parent, extensions.toList)
    case Backward =>
      new LeftExtendedExtensionSet(parent, extensions.toList)
  }
}

class ExtensionSetBuffer(parent: ExtensionSet, direction: Direction) {
  private val extensionSets = new ListBuffer[ExtendedExtensionSet]

  def append(extensionSet: ExtensionSet) = extensionSet match {
    case extensionSet: ExtendedExtensionSet =>
      if (extensionSet.parent == parent)
        extensionSets.append(extensionSet)
      else
        throw new IllegalArgumentException("ExtensionSetBuffer: extensionSet.parent != parent")
      
      extensionSet match {
        case _: LeftExtendedExtensionSet =>
          if (direction == Forward)
            throw new IllegalArgumentException("ExtensionSetBuffer: LeftExtendedExtensionSet appended to Forward buffer")
        case _: RightExtendedExtensionSet =>
          if (direction == Backward)
            throw new IllegalArgumentException("ExtensionSetBuffer: RightExtendedExtensionSet appended to Backward buffer")
      }
    case _ =>
      throw new IllegalArgumentException("ExtensionSetBuffer: extensionSet has no parent")
  }

  def toExtensionSet = {
    val builder = new ExtensionSetBuilder(parent, direction)

    for (extensionSet <- extensionSets; (pathPos, extension) <- extensionSet.extensionsList)
      builder.extend(pathPos, extension)

    builder.build
  }
}
