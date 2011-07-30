package org.wquery.model

object WQueryOrdering extends Ordering[Any] {
  def compare(left: Any, right: Any): Int = {
    (left, right) match {
      case (left: Synset, right: Synset) =>
        left.id compare right.id
      case (left: Sense, right: Sense) =>
        left.id compare right.id
      case (left: String, right: String) =>
        left compare right
      case (left: Int, right: Int) =>
        left compare right
      case (left: Int, right: Double) =>
        left.doubleValue compare right
      case (left: Double, right: Int) =>
        left compare right
      case (left: Double, right: Double) =>
        left compare right
      case (left: Boolean, right: Boolean) =>
        left compare right
      case (left: Arc, right: Arc) =>
        if (left.relation.name != right.relation.name)
            left.relation.name compare right.relation.name
        else if (left.from != right.from)
            left.from compare right.from
        else
            left.to compare right.to
      case _ =>
        throw new IllegalArgumentException("Objects " + left + " and " + right + " cannot be compared")
    }
  }
}

object WQueryListOrdering extends Ordering[List[Any]] {
  def compare(left: List[Any], right: List[Any]): Int = {
    if (left.isEmpty) {
      if (right.isEmpty) 0 else -1
    } else {
      if (right.isEmpty) {
        1
      } else {
        val res = WQueryOrdering.compare(left.head, right.head)

        if (res != 0) {
          res
        } else {
          compare(left.tail, right.tail)
        }
      }
    }
  }
}