package org.wquery

import model.DataSet
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

  private def emitted(dataSet: DataSet) = emitter.emit(Answer(wquery.wordNet, dataSet))

  @Test def planSingleTransformation() = {
    val path = planner ffor ("{car}.hypernym") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,VariableTemplate(List())),ContextRefOp(Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}),Forward,VariableTemplate(List()))")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}),Right),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1}),Backward,VariableTemplate(List())),VariableTemplate(List($__a, @_))),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,VariableTemplate(List())),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  // {car}.hypernym.{bus}

  // {plan}.hypernym$a.meronym[$a={xyz}]

}