package org.wquery

import model._
import scalaz._
import Scalaz._
import engine._
import operations.{Bindings, BindingsSchema}
import org.testng.annotations.Test
import planner.{PlanEvaluator, PathPlanGenerator}

class PlannerTests extends WQueryTestSuite {

  def pathFor(query: String) = {
    (wquery.parser.parse(query): @unchecked) match {
      case FunctionExpr(sort,FunctionExpr(distinct, ConjunctiveExpr(pathExpr @ PathExpr(steps, _)))) =>
        pathExpr.createPath(steps, wquery.wordNet.store.schema, BindingsSchema(), Context())
    }
  }

  def plansFor(query: String) = new PathPlanGenerator(pathFor(query)).plan(wquery.wordNet.store.schema, BindingsSchema())

  private def emitted(dataSet: DataSet) = emitter.emit(Answer(wquery.wordNet, dataSet))

  @Test def planSingleTransformation() {
    val plans = plansFor("{car}.hypernym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(source&synset^hypernym^destination&synset,Right),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planInvertedTransformation() {
    val plans = plansFor("{cab}.^hypernym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(cab))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,destination&synset^hypernym^source&synset,Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(destination&synset^hypernym^source&synset,Right),0,destination&synset^hypernym^source&synset,Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(cab))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planMultipleTransformations() {
    val plans = plansFor("{}.hypernym.partial_holonym.hypernym")
    println(plans)
    val forwardPlan = plans.find(_.toString == "BindOp(ExtendOp(ExtendOp(ExtendOp(FringeOp(source&synset^partial_holonym^destination&synset,Left),0,source&synset^partial_holonym^destination&synset,Forward,novars),0,source&synset^hypernym^destination&synset,Forward,novars),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(ExtendOp(ExtendOp(FringeOp(source&synset^partial_holonym^destination&synset,Right),0,source&synset^partial_holonym^destination&synset,Backward,novars),0,source&synset^hypernym^destination&synset,Forward,novars),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)").get

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planTransformationsUnion() {
    val plans = plansFor("{car}.hypernym|partial_holonym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Right),0,RelationUnionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset)),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planTransformationsComposition() {
    val plans = plansFor("{}.(hypernym.partial_holonym.hypernym)")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("BindOp(ExtendOp(FringeOp(RelationCompositionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset, source&synset^hypernym^destination&synset)),Right),0,RelationCompositionPattern(List(source&synset^hypernym^destination&synset, source&synset^partial_holonym^destination&synset, source&synset^hypernym^destination&synset)),Backward,novars),$__f@_)")
    emitted(result) should equal ("{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
  }

  @Test def planOneOrMoreTransformations() {
    val plans = plansFor("{car:3:n}.hypernym+")
    val forwardPlan = plans.find(_.toString == "ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Right),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{1,}),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planZeroOrMoreTransformations() {
    val plans = plansFor("{car:3:n}.hypernym*")
    val forwardPlan = plans.find(_.toString == "ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Right),0,QuantifiedRelationPattern(source&synset^hypernym^destination&synset,{0,}),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(3)), (pos,List(n))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveNodeFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}")
    val forwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveContextFilter() {
    val plans = plansFor("{}.hypernym[# = {cab:3}]")
    val forwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,novars),$__#),BinaryCondition(=,StepVariableRefOp($__#,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveVariableFilter() {
    val plans = plansFor("{}.hypernym$a[$a = {cab:3}]")
    val forwardPlan = plans.find(_.toString == "SelectOp(ExtendOp(FetchOp(synsets,List((source,List())),List(source)),0,source&synset^hypernym^destination&synset,Forward,$a),BinaryCondition(=,StepVariableRefOp($a,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Backward,$a),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings())

    emitted(forwardResult) should equal ("$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def verifyStats() {
    val stats = wquery.wordNet.store.schema.stats

    stats.fetchMaxCount(WordNet.SynsetSet, List((Relation.Source, Nil)), List(Relation.Source)) should equal (tuples.of("count({})").asValueOf[Int])
    stats.fetchMaxCount(WordNet.SenseSet, List((Relation.Source, Nil)), List(Relation.Source)) should equal (tuples.of("count(::)").asValueOf[Int])
    stats.fetchMaxCount(WordNet.WordSet, List((Relation.Source, Nil)), List(Relation.Source)) should equal (tuples.of("count('')").asValueOf[Int])
    stats.fetchMaxCount(WordNet.PosSet, List((Relation.Source, Nil)), List(Relation.Source)) should equal (tuples.of("count(possyms)").asValueOf[Int])
    stats.fetchMaxCount(WordNet.SenseToWordFormSenseNumberAndPos, List((Relation.Source, List("cab")),("num", List(3)), ("pos", List("n"))), List(Relation.Source)) should equal (tuples.of("count(cab.senses)").asValueOf[Int])

    stats.extendMaxCount(some(1), ArcPattern(
      wquery.wordNet.store.schema.relations.find(_.name == "hypernym"),
      ArcPatternArgument("source", None), List(ArcPatternArgument("destination", None))
    ), Forward).get should equal (tuples.of("max(from {}$a emit count($a.hypernym))").asValueOf[Int])

    stats.extendMaxCount(some(1), ArcPattern(
      wquery.wordNet.store.schema.relations.find(_.name == "hypernym"),
      ArcPatternArgument("source", None), List(ArcPatternArgument("destination", None))
    ), Backward).get should equal (tuples.of("max(from {}$a emit count($a.^hypernym))").asValueOf[Int])
  }

  @Test def choosePlanForPathWithHighlySelectiveNodeFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)")
    emitted(result) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
  }

  @Test def choosePlanForPathWithHighlySelectiveGenerator() {
    val plans = plansFor("{cab:3}.hypernym")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Forward,novars)")
    emitted(result) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanForPathWithHighlySelectiveFilterAndGenerator() {
    val plans = plansFor("{cab:3}.hypernym.{car:1}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("SelectOp(BindOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(car)), (num,List(1))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset)))))")
    emitted(result) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanByHighlySelectiveInnerFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}.hypernym")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("BindOp(ExtendOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(cab)), (num,List(3))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Forward,novars),0,source&synset^hypernym^destination&synset,Backward,novars),$__f@_)")
    emitted(result) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanByHighlySelectiveOuterNodes() {
    val plans = plansFor("{bus:4}.hypernym$a.^hypernym.{ambulance:1}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("NaturalJoinOp(ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(bus)), (num,List(4))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,source&synset^hypernym^destination&synset,Forward,$a),ExtendOp(ProjectOp(ExtendOp(FetchOp(literal,List((destination,List(ambulance)), (num,List(1))),List(source)),0,RelationUnionPattern(List(source&sense^synset^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))),0,destination&synset^hypernym^source&synset,Backward,novars))")
    emitted(result) should equal ("$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n")
  }

  @Test def backwardPlanWithTraverseThroughFilter() {
    val plans = plansFor("{}.hypernym[max(last(senses.sensenum)) > 2].hypernym.{car}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.store.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings())

    plan.toString should equal ("BindOp(SelectOp(ExtendOp(BindOp(SelectOp(BindOp(ExtendOp(FringeOp(source&synset^hypernym^destination&synset,Left),0,source&synset^hypernym^destination&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),ProjectOp(ExtendOp(FetchOp(words,List((source,List(car))),List(source)),0,RelationUnionPattern(List(source&string^synsets^destination&synset)),Forward,$__p),StepVariableRefOp($__p,Set(synset))))),$__#@_),0,source&synset^hypernym^destination&synset,Backward,novars),BinaryCondition(>,FunctionOp(max,FunctionOp(last,ExtendOp(ExtendOp(StepVariableRefOp($__#,Set(synset)),0,source&synset^senses^destination&sense,Forward,novars),0,source&sense^sensenum^destination&integer,Forward,novars))),ConstantOp(List(List(2))))),$__f@_)")
    emitted(result) should equal ("{ brougham:2:n } hypernym { sedan:1:n saloon:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ shooting brake:1:n } hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

}