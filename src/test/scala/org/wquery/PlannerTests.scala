package org.wquery

import model.DataSet
import scalaz._
import Scalaz._
import engine._
import operations.{Bindings, BindingsSchema}
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
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,source&synset^hypernym^destination&synset,Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(source&synset^hypernym^destination&synset,Right),0,source&synset^hypernym^destination&synset,Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planInvertedTransformation() = {
    val path = planner ffor ("{cab}.^hypernym") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(cab))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,destination&synset^hypernym^source&synset,Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(destination&synset^hypernym^source&synset,Right),0,destination&synset^hypernym^source&synset,Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(cab))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planMultipleTransformations() = {
    val path = planner ffor ("{}.hypernym.partial_holonym.hypernym") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ExtendOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,novars),0,source&synset^partial_holonym^destination&synset,Forward,novars),0,source&synset^hypernym^destination&synset,Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(ExtendOp(ExtendOp(FringeOp(source&synset^hypernym^destination&synset,Right),0,source&synset^hypernym^destination&synset,Backward,novars),0,source&synset^partial_holonym^destination&synset,Backward,novars),0,source&synset^hypernym^destination&synset,Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),FetchOp(synsets,List((source,List())),List(source))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planTransformationsUnion() = {
    val path = planner ffor ("{car}.hypernym|partial_holonym") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Right),0,RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planTransformationsComposition() = {
    val path = planner ffor ("{}.(hypernym.partial_holonym.hypernym)") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,RelationCompositionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset, source&synset^hypernym^destination&synset)),Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(RelationCompositionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset, source&synset^hypernym^destination&synset)),Right),0,RelationCompositionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset, source&synset^hypernym^destination&synset)),Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),FetchOp(synsets,List((source,List())),List(source))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planOneOrMoreTransformations() = {
    val path = planner ffor ("{car:3:n}.hypernym+") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Right),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planZeroOrMoreTransformations() = {
    val path = planner ffor ("{car:3:n}.hypernym*") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Forward,novars)")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Right),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveNodeFilter() = {
    val path = planner ffor ("{}.hypernym.{cab:3}") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("SelectOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,novars),BinaryCondition(in,ContextRefOp(Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,source&synset^hypernym^destination&synset,Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),FetchOp(synsets,List((source,List())),List(source))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveContextFilter() = {
    val path = planner ffor ("{}.hypernym[# = {cab:3}]") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("SelectOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,novars),BinaryCondition(=,ContextRefOp(Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,source&synset^hypernym^destination&synset,Backward,novars),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),FetchOp(synsets,List((source,List())),List(source))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveVariableFilter() = {
    val path = planner ffor ("{}.hypernym$a[$a = {cab:3}]") path

    val forwardPlan = path.walkForward(BindingsSchema(), 0, path.links.size - 1)
    val backwardPlan = path.walkBackward(wquery.wordNet.schema, BindingsSchema(), 0, path.links.size - 1)

    forwardPlan.toString should equal ("SelectOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,$a),BinaryCondition(=,StepVariableRefOp($a,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset)))))")
    backwardPlan.toString should equal ("SelectOp(BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,novars),ContextRefOp(Set(synset))),0,source&synset^hypernym^destination&synset,Backward,$a),$__a@_),BinaryCondition(in,StepVariableRefOp($__a,Set(synset)),FetchOp(synsets,List((source,List())),List(source))))")

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }
}