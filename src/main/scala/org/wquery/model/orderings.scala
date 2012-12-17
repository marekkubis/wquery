package org.wquery.model

object SynsetOrdering extends Ordering[Synset] {
  def compare(left: Synset, right: Synset) = {
    left.id compare right.id
  }
}

object SenseOrdering extends Ordering[Sense] {
  def compare(left: Sense, right: Sense) = {
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
  }
}

object ArcOrdering extends Ordering[Arc] {
  def compare(left: Arc, right: Arc) = {
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
  }
}

object AnyOrdering extends Ordering[Any] {
  // scalastyle:off cyclomatic.complexity
  def compare(left: Any, right: Any): Int = {
    (left, right) match {
      case (left: Synset, right: Synset) =>
        SynsetOrdering.compare(left, right)
      case (left: Sense, right: Sense) =>
        SenseOrdering.compare(left, right)
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
        ArcOrdering.compare(left, right)
      case (left, right) =>
        DataType.fromValue(left).rank compare DataType.fromValue(right).rank
    }
  }
  // scalastyle:on cyclomatic.complexity
}

object AnyListOrdering extends Ordering[List[Any]] {
  def compare(left: List[Any], right: List[Any]): Int = {
    if (left.isEmpty) {
      if (right.isEmpty) 0 else -1
    } else {
      if (right.isEmpty) {
        1
      } else {
        val res = AnyOrdering.compare(left.head, right.head)

        if (res != 0) {
          res
        } else {
          compare(left.tail, right.tail)
        }
      }
    }
  }
}
