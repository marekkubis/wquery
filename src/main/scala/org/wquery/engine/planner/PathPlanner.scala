package org.wquery.engine.planner

import org.wquery.engine.operations._
import scalaz._
import Scalaz._

class PathPlanner(val path: Path) {
  def plan(context: BindingsSchema) = {
    // TODO choose the best one after evaluation
    path.walkForward(context, 0, path.links.size - 1)
  }
}
