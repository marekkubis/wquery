package org.wquery

import org.testng.annotations.Test

class UpdatesTestSuite extends WQueryTestSuite {
  @Test def testAddSynsetNewSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update synsets += {xxx:1:n union yyy:2:n union zzz:3:n}" should equal ("(no result)\n")
    result(wupdate) of "{xxx}" should equal ("{ xxx:1:n yyy:2:n zzz:3:n }\n")
  }

  @Test def testAddSynsetMoveSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update synsets += {'man-eater':2:n}" should equal ("(no result)\n")
    result(wupdate) of "{'man-eating shark'}" should equal ("{ great white shark:1:n white shark:1:n man-eating shark:1:n Carcharodon carcharias:1:n }\n")
  }

  @Test def testAddSynsetTwoSynsets() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update synsets += {jjj:1:n} union {kkk:1:n}" should equal ("(no result)\n")
    result(wupdate) of "{jjj}" should equal ("{ jjj:1:n }\n")
  }

  @Test def testAddSensesOneSense() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update senses += newsense:1:n" should equal ("(no result)\n")
    result(wupdate) of "newsense:1:n" should equal ("newsense:1:n\n")
    result(wupdate) of "{newsense:1:n}" should equal ("{ newsense:1:n }\n")
  }

  @Test def testAddSensesMultipleSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "do update senses += newsense:2:n union newsense:3:n update {newsense:2:n} desc := `d1` update {newsense:3:n} desc := `d2` end" should equal ("(no result)\n")
    result(wupdate) of "{newsense:2:n}.desc" should equal ("{ newsense:2:n } desc d1\n")
    result(wupdate) of "{newsense:3:n}.desc" should equal ("{ newsense:3:n } desc d2\n")
  }

  @Test def testAddWords() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update words += newword" should equal ("(no result)\n")
    result(wupdate) of "newword" should equal ("newword\n")
    result(wupdate) of "newword.senses" should equal ("(no result)\n")
  }

  @Test def testAddPartOfSpeechSymbol() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update possyms += `z`" should equal ("(no result)\n")
    result(wupdate) of "possyms" should equal ("a\nn\ns\nv\nz\n")
  }

  @Test def testAddRelations() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of
      """
        |do
        |  update relations += `newrelation`
        |  update `newrelation` arguments += `src`, `synset`, 0
        |  update `newrelation` arguments += `dst`, `synset`, 1
        |end
      """.stripMargin should equal ("(no result)\n")
    result(wupdate) of "{car}.newrelation" should equal ("(no result)\n")
  }

  @Test def testAddLinksByName() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update {car:1} hypernym += {person:1}" should equal ("(no result)\n")
    result(wupdate) of "{car:1}.hypernym" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n")
  }

  @Test def testAddLinksByNameAndArguments() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update {person:2} dst^hypernym^src += {car:1}" should equal ("(no result)\n")
    result(wupdate) of "{car:1}.hypernym" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { person:2:n }\n")
  }

  @Test def testAddLinksFunctionalForPropertySupport() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update `desc`, `src` properties += `functional`" should equal ("(no result)\n")
    result(wupdate) of "update {person:1} desc += `hhhhhhhhhhhhh`" should startWith ("ERROR: Update breaks property 'functional' of relation 'desc' on argument 'src'")
    result(wupdate) of "update `desc`, `src` properties -= `functional`" should equal ("(no result)\n")
  }

  @Test def testAddLinksSymmetricPropertyPreserved() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update pair_properties += `similar`, `src`, `dst`, `symmetric`, `preserve`" should equal ("(no result)\n")
    result(wupdate) of "update {apple:1:n} similar := {apple:2:n}" should startWith ("ERROR: Update breaks property 'symmetric' of relation 'similar'")
    result(wupdate) of "update pair_properties += `similar`, `src`, `dst`, `symmetric`, `restore`" should equal ("(no result)\n")
    result(wupdate) of "update {apple:1:n} similar := {apple:2:n}" should equal ("(no result)\n")
    result(wupdate) of "{apple}.similar" should equal ("{ apple:1:n } similar { apple:2:n orchard apple tree:1:n Malus pumila:1:n }\n{ apple:2:n orchard apple tree:1:n Malus pumila:1:n } similar { apple:1:n }\n")
  }

  @Test def testAddLinksTransitiveAntiSymmetricPropertyPreserved() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{car:1:n} hypernym += {car:1:n}" startsWith "ERROR: Update breaks property 'transitive antisymmetry' of relation 'hypernym'"
    result(wupdate) of "{'motor vehicle':1:n} hypernym += {car:1:n}" startsWith "ERROR: Update breaks property 'transitive antisymmetry' of relation 'hypernym'"
    result(wupdate) of "{'self-propelled vehicle':1:n} hypernym += {car:1:n}" startsWith "ERROR: Update breaks property 'transitive antisymmetry' of relation 'hypernym'"
  }

  @Test def testRemoveSense() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update senses -= car:1:n" should equal ("(no result)\n")
    result(wupdate) of "car:1:n" should equal ("(no result)\n")
    result(wupdate) of "{car:1:n}" should equal ("(no result)\n")
    result(wupdate) of "car.senses" should equal ("car senses car:2:n\ncar senses car:3:n\ncar senses car:4:n\ncar senses car:5:n\n")
    result(wupdate) of "{car}" should equal ("{ cable car:1:n car:5:n }\n{ auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ car:4:n elevator car:1:n }\n{ car:3:n gondola:3:n }\n")
  }

  @Test def testRemoveWord() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update words -= car" should equal ("(no result)\n")
    result(wupdate) of "car" should equal ("(no result)\n")
    result(wupdate) of "car:1:n" should equal ("(no result)\n")
    result(wupdate) of "{car}" should equal ("(no result)\n")
    result(wupdate) of "{auto:1:n}" should equal ("{ auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
    result(wupdate) of "car.senses" should equal ("(no result)\n")
  }

  @Test def testRemoveSynset() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update synsets -= {car:5:n}" should equal ("(no result)\n")
    result(wupdate) of "{car}" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ car:4:n elevator car:1:n }\n{ car:3:n gondola:3:n }\n")
    result(wupdate) of "car.senses" should equal ("car senses car:1:n\ncar senses car:2:n\ncar senses car:3:n\ncar senses car:4:n\n")
  }

  @Test def testRemovePartOfSpeechSymbol() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "cold:1:a" should equal ("cold:1:a\n")
    result(wupdate) of "update possyms -= `a`" should equal ("(no result)\n")
    result(wupdate) of "cold:1:a" should equal ("(no result)\n")
  }

  @Test def testRemoveNodePreservesTransitivityProperty() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{entity}.^hypernym" should equal ("{ entity:1:n } ^hypernym { physical entity:1:n }\n")
    result(wupdate) of "{entity}.^hypernym.^hypernym" should equal ("{ entity:1:n } ^hypernym { physical entity:1:n } ^hypernym { object:1:n physical object:1:n }\n{ entity:1:n } ^hypernym { physical entity:1:n } ^hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
    result(wupdate) of "update `hypernym`, `src`, `dst` pair_properties += `transitive`, `preserve`" should equal ("(no result)\n")
    result(wupdate) of "update synsets -= {'physical entity'}" should startWith ("ERROR: Update breaks property 'transitivity' of relation 'hypernym'")
    result(wupdate) of "update `hypernym`, `src`, `dst` pair_properties += `transitive`, `restore`" should equal ("(no result)\n")
    result(wupdate) of "update synsets -= {'physical entity'}" should equal ("(no result)\n")
    result(wupdate) of "{entity}.^hypernym" should equal ("{ entity:1:n } ^hypernym { object:1:n physical object:1:n }\n{ entity:1:n } ^hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n")
    result(wupdate) of "update `hypernym`, `src`, `dst` pair_properties -= `transitive`, `restore`" should equal ("(no result)\n")
  }

  @Test def testRemoveRelation() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "count({}.del_this_rel)" should equal ("1\n")
    result(wupdate) of "update relations -= `del_this_rel`" should equal ("(no result)\n")
    result(wupdate) of "{}.del_this_rel" should startWith ("ERROR: Arc expression del_this_rel references an unknown relation or argument")
  }

  @Test def testRemoveRelationMandatoryOne() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update relations -= `senses`" should startWith ("ERROR: Cannot remove the mandatory relation 'senses'")
  }

  @Test def testRemoveLinks() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update {cab:3} hypernym -= {car}" should equal ("(no result)\n")
    result(wupdate) of "{cab:3:n}.hypernym" should equal ("(no result)\n")
  }

  @Test def testRemoveLinksRequiredPropertyBreak() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update `desc`, `src` properties := `required`" should equal ("(no result)\n")
    result(wupdate) of "update {apple:1:n} desc -= last({apple:1:n}.desc)" should startWith ("ERROR: Update breaks property 'required_by' of relation 'desc' on argument 'src'")
    result(wupdate) of "update `desc`, `src` properties -= `required`" should equal ("(no result)\n")
  }

  @Test def testRemoveLinksSymmetricPropertyPreserved() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update pair_properties += `similar`, `src`, `dst`, `symmetric`, `restore`" should equal ("(no result)\n")
    result(wupdate) of "update {orange:2:n} similar := {orange:4:n}" should equal ("(no result)\n")
    result(wupdate) of "{orange}.similar" should equal ("{ orange:2:n orangeness:1:n } similar { orange:4:n }\n{ orange:4:n } similar { orange:2:n orangeness:1:n }\n")
    result(wupdate) of "update {orange:4:n} similar -= {orange:2:n}" should equal ("(no result)\n")
    result(wupdate) of "{orange}.similar" should equal ("(no result)\n")
  }

  @Test def testRemoveLinksByNameAndArguments() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update {racer:2} dst^hypernym^src -= {finisher:5}" should equal ("(no result)\n")
    result(wupdate) of "{finisher:5}.hypernym" should equal ("(no result)\n")
  }

  @Test def testRemoveLinksMultiArgumentRelationOneArgumentSet() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update minicab:1:n src^literal^num^pos -= 1,`n`" should equal ("(no result)\n")
    result(wupdate) of "(minicab:1:n).literal" should equal ("(no result)\n")
  }

  @Test def testSetLinks() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update {apple:1:n} hypernym += {person:2}" should equal ("(no result)\n")
    result(wupdate) of "update {apple:1:n} hypernym += {person:3}" should equal ("(no result)\n")
    result(wupdate) of "{apple:1:n}.hypernym" should equal ("{ apple:1:n } hypernym { person:2:n }\n{ apple:1:n } hypernym { person:3:n }\n")
    result(wupdate) of "update {apple:1:n} hypernym := {person:2}" should equal ("(no result)\n")
    result(wupdate) of "{apple:1:n}.hypernym" should equal ("{ apple:1:n } hypernym { person:2:n }\n")
 }

  @Test def testSetLinksFunctionalPropertyBreak() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update `desc`, `src` properties := `functional`" should equal ("(no result)\n")
    result(wupdate) of "update {apple:1} desc := `aaa`" should equal ("(no result)\n")
    result(wupdate) of "{apple:1}.desc" should equal ("{ apple:1:n } desc aaa\n")
    result(wupdate) of "update {apple:1} desc := `bbb` union `ddd`" should startWith ("ERROR: Update breaks property 'functional' of relation 'desc' on argument 'src'")
    result(wupdate) of "{apple:1}.desc" should equal ("{ apple:1:n } desc aaa\n")
    result(wupdate) of "update `desc`, `src` properties -= `functional`" should equal ("(no result)\n")
  }

  @Test def testSetLinksRequiredByPropertyBreak() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update `desc`, `src` properties += `required`" should equal ("(no result)\n")
    result(wupdate) of "update last({person}.desc) dst^desc^src := {car:1}" should startWith ("ERROR: Update breaks property 'required_by' of relation 'desc' on argument 'src'")
    result(wupdate) of "update `desc`, `src` properties -= `required`" should equal ("(no result)\n")
  }

  @Test def testSetLinksRemoveLinks() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{}.member_holonym" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } member_holonym { people:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } member_holonym { fleet:2:n }\n")
    result(wupdate) of "update {} member_holonym := <>" should equal ("(no result)\n")
    result(wupdate) of "{}.member_holonym" should equal ("(no result)\n")
  }

  @Test def testSetSynsets() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{unit:6:n}" should equal ("{ whole:2:n unit:6:n }\n")
    result(wupdate) of "update synsets := {}$_a[not unit:6:n in $_a.senses] union {pppp:1:n}" should equal ("(no result)\n")
    result(wupdate) of "{unit:6:n}" should equal ("(no result)\n")
  }

  @Test def testSetPartOfSpeechSymbol() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update possyms := `a` union `n` union `v` union `e`" should equal ("(no result)\n")
    result(wupdate) of "possyms" should equal ("a\ne\nn\nv\n")
  }

  @Test def testSetWords() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "jeep" should equal ("jeep\n")
    result(wupdate) of "update words := ''$_a[$_a != jeep]" should equal ("(no result)\n")
    result(wupdate) of "jeep" should equal ("(no result)\n")
  }

  @Test def testSetSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "set:2:v" should equal ("set:2:v\n")
    result(wupdate) of "update senses := ::$_a[$_a != set:2:v]" should equal ("(no result)\n")
    result(wupdate) of "set:2:v" should equal ("(no result)\n")
  }

  @Test def testMergeSynsets() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "merge {coupe:1:n} union {compact:3}" should equal ("(no result)\n")
    result(wupdate) of "{coupe:1:n}" should equal ("{ coupe:1:n compact:3:n compact car:1:n }\n")
    result(wupdate) of "{compact:3:n}.hypernym" should equal ("{ coupe:1:n compact:3:n compact car:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  }

  @Test def testMergeSynsetsInTransitiveRelationTree() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update pair_properties += `hypernym`, `src`, `dst`, `antisymmetric`, `preserve`" should equal ("(no result)\n")
    result(wupdate) of "update pair_properties += `hypernym`, `src`, `dst`, `transitive`, `preserve`" should equal ("(no result)\n")
    result(wupdate) of "merge {`motor vehicle`} union {`wheeled vehicle`}" should startWith ("ERROR: Update breaks property 'transitive antisymmetry' of relation 'hypernym'")
    result(wupdate) of "update pair_properties -= `hypernym`, `src`, `dst`, `antisymmetric`, `preserve`" should equal ("(no result)\n")
    result(wupdate) of "update pair_properties -= `hypernym`, `src`, `dst`, `transitive`, `preserve`" should equal ("(no result)\n")
  }

  @Test def testMergeSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "merge 'funny wagon':1:n union 'beach wagon':1:n" should equal ("(no result)\n")
    result(wupdate) of "{'funny wagon'}.hypernym" should equal ("{ funny wagon:1:n beach wagon:1:n } hypernym { ambulance:1:n }\n{ funny wagon:1:n beach wagon:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
    result(wupdate) of "count({}[count(senses) = 0])" should equal ("0\n")
  }

  @Test def testMergeSynsetsAndSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "merge {minicab:1:n} union 'gypsy cab':1:n" should equal ("(no result)\n")
    result(wupdate) of "{minicab:1:n}.hypernym" should equal ("{ minicab:1:n gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ minicab:1:n gypsy cab:1:n } hypernym { minicar:1:n }\n")
  }

  @Test def testAddRemoveAlias() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update aliases += `hypernym`, `dst`, `src`, `hypo`" should equal ("(no result)\n")
    result(wupdate) of "{taxi:1:n}.hypo" should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n")
    result(wupdate) of "{taxi:1:n}.^hypo" should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
    result(wupdate) of "update aliases -= `hypernym`, `dst`, `src`, `hypo`" should equal ("(no result)\n")
    result(wupdate) of "{minicab:1:n}.hypo" should startWith ("ERROR: Arc expression hypo references an unknown relation or argument")
  }

  @Test def testAddMultipleAliases() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "update aliases += `member_holonym`, `dst`, `src`, `holonym`" should equal ("(no result)\n")
    result(wupdate) of "update aliases += `partial_holonym`, `dst`, `src`, `holonym`" should equal ("(no result)\n")
    result(wupdate) of "count({}.holonym)" should equal ("9\n")
  }
}