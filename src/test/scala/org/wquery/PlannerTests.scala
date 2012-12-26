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
        pathExpr.createPath(steps, wquery.wordNet.schema, BindingsSchema(), Context())
    }
  }

  def plansFor(query: String) = new PathPlanGenerator(pathFor(query)).plan(wquery.wordNet.schema, BindingsSchema())

  private def emitted(dataSet: DataSet) = emitter.emit(Answer(wquery.wordNet, dataSet))

  @Test def planSingleTransformation() {
    val plans = plansFor("{car}.hypernym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(SynsetFetchOp(FetchOp(words,List((src,List(car))),List(src))),src&synset^hypernym^dst&synset,Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(src&synset^hypernym^dst&synset,Right),src&synset^hypernym^dst&synset,Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),SynsetFetchOp(FetchOp(words,List((src,List(car))),List(src)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planInvertedTransformation() {
    val plans = plansFor("{cab}.^hypernym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(SynsetFetchOp(FetchOp(words,List((src,List(cab))),List(src))),dst&synset^hypernym^src&synset,Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(dst&synset^hypernym^src&synset,Right),dst&synset^hypernym^src&synset,Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),SynsetFetchOp(FetchOp(words,List((src,List(cab))),List(src)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planMultipleTransformations() {
    val plans = plansFor("{}.hypernym.partial_holonym.hypernym")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("ExtendOp(ExtendOp(ExtendOp(FetchOp(synsets,List((src,List())),List(src)),src&synset^hypernym^dst&synset,Forward,novars),src&synset^partial_holonym^dst&synset,Forward,novars),src&synset^hypernym^dst&synset,Forward,novars)")
    emitted(result) should equal ("{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
  }

  @Test def planTransformationsUnion() {
    val plans = plansFor("{car}.hypernym|partial_holonym")
    val forwardPlan = plans.find(_.toString == "ExtendOp(SynsetFetchOp(FetchOp(words,List((src,List(car))),List(src))),RelationUnionPattern(List(src&synset^hypernym^dst&synset, src&synset^partial_holonym^dst&synset)),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(RelationUnionPattern(List(src&synset^hypernym^dst&synset, src&synset^partial_holonym^dst&synset)),Right),RelationUnionPattern(List(src&synset^hypernym^dst&synset, src&synset^partial_holonym^dst&synset)),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),SynsetFetchOp(FetchOp(words,List((src,List(car))),List(src)))))").get

    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planTransformationsComposition() {
    val plans = plansFor("{}.(hypernym.partial_holonym.hypernym)")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("ExtendOp(FetchOp(synsets,List((src,List())),List(src)),RelationCompositionPattern(List(src&synset^hypernym^dst&synset, src&synset^partial_holonym^dst&synset, src&synset^hypernym^dst&synset)),Forward,novars)")
    emitted(result) should equal ("{ compartment:2:n } hypernym { room:1:n } partial_holonym { building:1:n edifice:1:n } hypernym { structure:1:n construction:4:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:2:n } hypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n } partial_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
  }

  @Test def planOneOrMoreTransformations() {
    val plans = plansFor("{car:3:n}.hypernym+")
    val forwardPlan = plans.find(_.toString == "ExtendOp(SynsetFetchOp(FetchOp(senses,List((src,List(car:3:n))),List(src))),QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{1,}),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{1,}),Right),QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{1,}),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),SynsetFetchOp(FetchOp(senses,List((src,List(car:3:n))),List(src)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planZeroOrMoreTransformations() {
    val plans = plansFor("{car:3:n}.hypernym*")
    val forwardPlan = plans.find(_.toString == "ExtendOp(SynsetFetchOp(FetchOp(senses,List((src,List(car:3:n))),List(src))),QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{0,}),Forward,novars)").get
    val backwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FringeOp(QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{0,}),Right),QuantifiedRelationPattern(src&synset^hypernym^dst&synset,{0,}),Backward,novars),$__f@_),BinaryCondition(in,StepVariableRefOp($__f,Set(synset)),SynsetFetchOp(FetchOp(senses,List((src,List(car:3:n))),List(src)))))").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ car:3:n gondola:3:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n } hypernym { area:5:n } hypernym { structure:1:n construction:4:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveNodeFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}")
    val forwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FetchOp(synsets,List((src,List())),List(src)),src&synset^hypernym^dst&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Backward,novars),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveContextFilter() {
    val plans = plansFor("{}.hypernym[# = {cab:3}]")
    val forwardPlan = plans.find(_.toString == "SelectOp(BindOp(ExtendOp(FetchOp(synsets,List((src,List())),List(src)),src&synset^hypernym^dst&synset,Forward,novars),$__#),BinaryCondition(=,StepVariableRefOp($__#,Set(synset)),SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Backward,novars),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def planPathWithHighlySelectiveVariableFilter() {
    val plans = plansFor("{}.hypernym$a[$a = {cab:3}]")
    val forwardPlan = plans.find(_.toString == "SelectOp(ExtendOp(FetchOp(synsets,List((src,List())),List(src)),src&synset^hypernym^dst&synset,Forward,$a),BinaryCondition(=,StepVariableRefOp($a,Set(synset)),SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src)))))").get
    val backwardPlan = plans.find(_.toString == "BindOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Backward,$a),$__f@_)").get
    val forwardResult = forwardPlan.evaluate(wquery.wordNet, Bindings(), Context())
    val backwardResult = backwardPlan.evaluate(wquery.wordNet, Bindings(), Context())

    emitted(forwardResult) should equal ("$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n$a={ cab:3:n hack:5:n taxi:1:n taxicab:1:n } { minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
    assert(forwardResult mequal backwardResult)
  }

  @Test def verifyStats() {
    val stats = wquery.wordNet.schema.stats

    stats.fetchMaxCount(WordNet.SynsetSet, List((Relation.Src, Nil)), List(Relation.Src)) should equal (tuples.of("count({})").asValueOf[Int])
    stats.fetchMaxCount(WordNet.SenseSet, List((Relation.Src, Nil)), List(Relation.Src)) should equal (tuples.of("count(::)").asValueOf[Int])
    stats.fetchMaxCount(WordNet.WordSet, List((Relation.Src, Nil)), List(Relation.Src)) should equal (tuples.of("count('')").asValueOf[Int])
    stats.fetchMaxCount(WordNet.PosSet, List((Relation.Src, Nil)), List(Relation.Src)) should equal (tuples.of("count(possyms)").asValueOf[Int])
    stats.fetchMaxCount(WordNet.SenseToWordFormSenseNumberAndPos, List((Relation.Src, List("cab")),("num", List(3)), ("pos", List("n"))), List(Relation.Src)) should equal (tuples.of("count(cab.senses)").asValueOf[Int])

    stats.extendMaxCount(some(1), ArcPattern(
      wquery.wordNet.schema.relations.find(_.name == "hypernym"),
      ArcPatternArgument("src", None), List(ArcPatternArgument("dst", None))
    ), Forward).get should equal (tuples.of("max(from {}$a emit count($a.hypernym))").asValueOf[Int])

    stats.extendMaxCount(some(1), ArcPattern(
      wquery.wordNet.schema.relations.find(_.name == "hypernym"),
      ArcPatternArgument("src", None), List(ArcPatternArgument("dst", None))
    ), Backward).get should equal (tuples.of("max(from {}$a emit count($a.^hypernym))").asValueOf[Int])
  }

  @Test def choosePlanForPathWithHighlySelectiveNodeFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("BindOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Backward,novars),$__f@_)")
    emitted(result) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n")
  }

  @Test def choosePlanForPathWithHighlySelectiveGenerator() {
    val plans = plansFor("{cab:3}.hypernym")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Forward,novars)")
    emitted(result) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanForPathWithHighlySelectiveFilterAndGenerator() {
    val plans = plansFor("{cab:3}.hypernym.{car:1}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("SelectOp(BindOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),SynsetFetchOp(FetchOp(literal,List((dst,List(car)), (num,List(1))),List(src)))))")
    emitted(result) should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanByHighlySelectiveInnerFilter() {
    val plans = plansFor("{}.hypernym.{cab:3}.hypernym")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("BindOp(ExtendOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(cab)), (num,List(3))),List(src))),src&synset^hypernym^dst&synset,Forward,novars),src&synset^hypernym^dst&synset,Backward,novars),$__f@_)")
    emitted(result) should equal ("{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def choosePlanByHighlySelectiveOuterNodes() {
    val plans = plansFor("{bus:4}.hypernym$a.^hypernym.{ambulance:1}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("NaturalJoinOp(ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(bus)), (num,List(4))),List(src))),src&synset^hypernym^dst&synset,Forward,$a),ExtendOp(SynsetFetchOp(FetchOp(literal,List((dst,List(ambulance)), (num,List(1))),List(src))),dst&synset^hypernym^src&synset,Backward,novars))")
    emitted(result) should equal ("$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n")
  }

  @Test def backwardPlanWithTraverseThroughFilter() {
    val plans = plansFor("{}.hypernym[distinct(max(last(senses.sensenum))) > 2].hypernym.{car}")
    val plan = PlanEvaluator.chooseBest(wquery.wordNet.schema, plans)
    val result = plan.evaluate(wquery.wordNet, Bindings(), Context())

    plan.toString should equal ("SelectOp(BindOp(ExtendOp(SelectOp(BindOp(ExtendOp(FetchOp(synsets,List((src,List())),List(src)),src&synset^hypernym^dst&synset,Forward,novars),$__#),BinaryCondition(>,FunctionOp(distinct,FunctionOp(max,FunctionOp(last,ExtendOp(ExtendOp(StepVariableRefOp($__#,Set(synset)),src&synset^senses^dst&sense,Forward,novars),src&sense^sensenum^dst&integer,Forward,novars)))),ConstantOp(List(List(2))))),src&synset^hypernym^dst&synset,Forward,novars),$__#),BinaryCondition(in,StepVariableRefOp($__#,Set(synset)),SynsetFetchOp(FetchOp(words,List((src,List(car))),List(src)))))")
    emitted(result) should equal ("{ shooting brake:1:n } hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ brougham:2:n } hypernym { sedan:1:n saloon:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

}