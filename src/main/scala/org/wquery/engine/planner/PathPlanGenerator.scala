package org.wquery.engine.planner

import org.wquery.engine.operations._
import scalaz._
import Scalaz._
import org.wquery.model.WordNetSchema

class PathPlanGenerator(path: Path) {
  def plan(wordNet: WordNetSchema, bindings: BindingsSchema) = {
    List(
      path.walkForward(bindings, 0, path.links.size - 1),
      path.walkBackward(wordNet, bindings, 0, path.links.size - 1)
    )
  }
}
