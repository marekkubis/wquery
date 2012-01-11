package org.wquery

import scalaz._
import Scalaz._
import engine._
import org.testng.annotations.Test

class PlannerTests extends WQueryTestSuite {
  class PlannerFor {
    def ffor(query: String) = {
      (wquery.parser.parse(query): @unchecked) match {
        case FunctionExpr(sort,FunctionExpr(distinct, ConjunctiveExpr(pathExpr @ PathExpr(steps, _)))) =>
          pathExpr.createPlanner(steps, wquery.wordNet.schema, BindingsSchema(), Context())
      }
    }
  }

  val planner = new PlannerFor

  @Test def planSingleTransformation() = {
    val path = planner ffor ("{car}.hypernym") path

    path.walkForward(BindingsSchema(), 0, path.links.size - 1).toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,VariableTemplate(List())),ContextRefOp(Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}),Forward,VariableTemplate(List()))")
  }

  // {car}.hypernym.{bus}

  // {plan}.hypernym$a.meronym[$a={xyz}]

}