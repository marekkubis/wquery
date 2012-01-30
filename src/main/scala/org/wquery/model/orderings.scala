package org.wquery.model

object WQueryOrdering extends Ordering[Any] {
  def compare(left: Any, right: Any): Int = {
    (left, right) match {
      case (left: Synset, right: Synset) =>
        left.id compare right.id
      case (left: Sense, right: Sense) =>
        val wordFormComparison = left.wordForm compare right.wordForm

        if (wordFormComparison == 0) {
          val senseNumberComparison = left.senseNumber compare right.senseNumber

          if (senseNumberComparison == 0) {
            left.pos compare right.pos
          } else {
            senseNumberComparison
          }
        } else {
          wordFormComparison
        }
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
        val nameComparison = left.relation.name compare right.relation.name
        if (nameComparison != 0)
          nameComparison
        else{
          val fromComparison = left.from compare right.from

          if (fromComparison != 0) {
            fromComparison
          } else {
            left.to compare right.to
          }
        }
      case (left, right) =>
        DataType.fromValue(left).rank compare DataType.fromValue(right).rank
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