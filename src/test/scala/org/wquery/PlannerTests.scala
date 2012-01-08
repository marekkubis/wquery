package org.wquery

import scalaz._
import Scalaz._
import engine._
import org.testng.annotations.Test

class PlannerTests extends WQueryTestSuite {
  class Planner {
    def of(query: String) = {
      (wquery.parser.parse(query): @unchecked) match {
        case FunctionExpr(sort,FunctionExpr(distinct, ConjunctiveExpr(pathExpr @ PathExpr(steps, _)))) =>
          pathExpr.createPlanner(steps, wquery.wordNet.schema, BindingsSchema(), Context())
      }
    }
  }

  val planner = new Planner

  @Test def planSingleTransformation() = {
    val planBuilder = planner of ("{car}.hypernym")

    planBuilder.steps.size should equal (3)
    planBuilder.steps(0).toString should equal ("node(Some(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,VariableTemplate(List())),ContextRefOp(Set(synset)))))")
    planBuilder.steps(1).toString should equal ("link(0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}))")
    planBuilder.steps(2).toString should equal ("node(None)")

    planBuilder.walkForward(0, planBuilder.steps.size - 1).toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,VariableTemplate(List())),ContextRefOp(Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}),Forward,VariableTemplate(List()))")
//    planBuilder.walkBackward(0, planBuilder.steps.size - 1).toString should equal ("")
  }

  // {car}.hypernym.{bus}

  // {plan}.hypernym$a.meronym[$a={xyz}]

}