package org.wquery

import org.testng.annotations.Test

class FunctionalTestSuite extends WQueryTestSuite {

  //
  // Functional test suite is based on the corpus of wordnet-specific queries that
  // has been collected as a part of my PhD Thesis (http://hdl.handle.net/10593/6372).
  // The queries have been adapted to the sample wordnet distributed with WQuery,
  // but the correspondence with the query numbers in the thesis is maintained.
  //

  //
  // Use Case I: Search for synsets and related data
  //

  // Q01
  @Test def searchForSynsetsThatContainSpecificWord() = result of "{person}" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")

  // Q02
  @Test def searchForVerbSynsetsThatContainSpecificWord() = result of "{set}[senses.pos = `v`]" should equal ("{ determine:3:v set:2:v }\n")

  // Q03
  @Test def searchForASynsetThatContainsASpecificWordSense() = result of "{person:1:n}" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q04
  @Test def searchForSynsetsWhichWordSensesContainRsonString() = result of "{''$a[$a =~ `rson`]}" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")
  // alternative: {"rson"}

  // Q05
  @Test def searchForWordsThatContainRsonStringAndBelongToSomeNounSynsets() = result of "''$a[$a =~ `rson` and not empty(senses[synset.senses.pos = `n`])]" should equal ("$a=person person\n")

  // Q06
  @Test def searchForASynsetWithAGivenIdentifier() = result of "{}[id = `100007846`]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q07
  @Test def findAdjectiveSynsets() = result of "{}[senses.pos = `a`]" should equal ("{ cold:1:a }\n{ cold:2:a }\n{ zymotic:1:a zymolytic:1:a }\n")

  // Q08
  @Test def findSynsetsThatHaveASpecificNote() = result of "{}[snote = `testnote`]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q09
  @Test def findBaseConcepts() = result of "{}[bcs]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  // Q10
  @Test def findSynsetsThatArePredecessorsOfASynsetWithASpecificIdentifierWithRegardToAnySemanticRelation() = result of "{}[id = `100007846`].^_[issynset(#)]<#>" should equal ("{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n{ person:2:n }\n")

  // Q11: Find synsets that fulfil a query that is a combination of some of the queries Q1, Q3, Q4 and Q6-Q10 combined together with logical 'and' and 'or'.
  // Q11a
  @Test def findBaseConceptsOrAdjectiveSynsets() = result of "{}[bcs] union {}[pos = `a`]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ cold:1:a }\n{ cold:2:a }\n{ zymotic:1:a zymolytic:1:a }\n")
  // Q11b
  @Test def findBaseConceptsAndNounSynsets() = result of "{}[bcs] intersect {}[pos = `n`]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  // Q12: find meronyms of synsets determined using Q2 or Q3
  // Q12a(Q2)
  @Test def findMeronymsOfNounSynsetsThatContainSpecificWord() = result of "{person}[pos = `n`].^partial_holonym<#>" should equal ("{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n")
  // Q12b(Q3)
  @Test def findMeronymsOfASynsetThatContainsASpecificWordSense() = result of "{person:1:n}.^partial_holonym<#>" should equal ("{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n")

  // Q13: find trees of transitive hyponyms of the synsets determined using Q2 or Q3
  // Q13a - paths
  @Test def findAllPathsThatLinkNounSynsetsThatContainASpecificWordWithTheirHyponyms() = result of "{car}[pos = `n`].^hypernym+" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n } ^hypernym { funny wagon:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } ^hypernym { shooting brake:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } ^hypernym { panda car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n } ^hypernym { berlin:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { finisher:5:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { stock car:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n } ^hypernym { brougham:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n")
  @Test def findAllPathsThatLinksASynsetThatContainsASpecificWordSenseWithItsHyponyms() = result of "{car:1:n}.^hypernym+" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n } ^hypernym { funny wagon:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } ^hypernym { shooting brake:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } ^hypernym { panda car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n } ^hypernym { berlin:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { finisher:5:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { stock car:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n } ^hypernym { brougham:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n")
  // Q13b - edges
  @Test def findAllEdgesThatLinkNounSynsetsThatContainsASpecificWordWithTheirHyponyms() = result of "({car}[pos = `n`].^hypernym+$a)<$a>.^hypernym" should equal ("{ ambulance:1:n } ^hypernym { funny wagon:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } ^hypernym { shooting brake:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } ^hypernym { panda car:1:n }\n{ limousine:1:n limo:1:n } ^hypernym { berlin:3:n }\n{ minicar:1:n } ^hypernym { minicab:1:n }\n{ racer:2:n race car:1:n racing car:1:n } ^hypernym { finisher:5:n }\n{ racer:2:n race car:1:n racing car:1:n } ^hypernym { stock car:2:n }\n{ sedan:1:n saloon:3:n } ^hypernym { brougham:2:n }\n")
  @Test def findAllEdgesThatLinksASynsetThatContainsASpecificWordSenseWithItsHyponyms() = result of "({car:1:n}.^hypernym+$a)<$a>.^hypernym" should equal ("{ ambulance:1:n } ^hypernym { funny wagon:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } ^hypernym { shooting brake:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } ^hypernym { panda car:1:n }\n{ limousine:1:n limo:1:n } ^hypernym { berlin:3:n }\n{ minicar:1:n } ^hypernym { minicab:1:n }\n{ racer:2:n race car:1:n racing car:1:n } ^hypernym { finisher:5:n }\n{ racer:2:n race car:1:n racing car:1:n } ^hypernym { stock car:2:n }\n{ sedan:1:n saloon:3:n } ^hypernym { brougham:2:n }\n")

  // Q14: find hyponyms of hypernyms of the synsets determined using Q2 or Q3
  @Test def findHyponymsOfHypernymsOfNounSynsetsThatContainASpecificWord() = result of "{person}[pos = `n`].hypernym.^hypernym<#>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")
  @Test def findHyponymsOfHypernymsOfASynsetThatContainsASpecificWordSense() = result of "{person:1:n}.hypernym.^hypernym<#>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q15: Find antonyms of a given word sense/word
  // Q15.1: word sense
  @Test def findAntonymsOfAGivenWordSense() = result of "generated:2:n.antonym<#>" should equal ("generated:1:n\n")
  // Q15.2: word
  @Test def findAntonymsOfAGivenWord() = result of "generated.senses.antonym<#>" should equal ("generated:1:n\n")

  // Q16: For a given word or word sense find a disjunction of some queries from Q12-Q15
  // Q16a
  @Test def findMeronymsAndHyponymsOfHypernymsOfASynsetThatContainsASpecificWordSense() = result of "{car:1:n}.^partial_holonym<#> union {person:1:n}.hypernym.^hypernym<#>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q17
  @Test def findSensesOfAnEnglishSynsetWithAGivenIdentifier() = result of "{}[id = `100007846` and lang=`en`].senses<#>" should equal ("individual:1:n\nmortal:1:n\nperson:1:n\nsomebody:1:n\nsomeone:1:n\nsoul:2:n\n")

  // Q18
  @Test def findHyponymsOfAnEnglishSynsetWithASpecifiedIdentifier() = result of "{}[id = `102958343` and lang=`en`].^hypernym<#>" should equal ("{ ambulance:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ compact:3:n compact car:1:n }\n{ convertible:1:n }\n{ coupe:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ electric:1:n electric automobile:1:n electric car:1:n }\n{ gas guzzler:1:n }\n{ hardtop:1:n }\n{ hatchback:1:n }\n{ horseless carriage:1:n }\n{ hot rod:1:n hot-rod:1:n }\n{ jeep:1:n landrover:1:n }\n{ limousine:1:n limo:1:n }\n{ loaner:2:n }\n{ minicar:1:n }\n{ minivan:1:n }\n{ Model T:1:n }\n{ pace car:1:n }\n{ racer:2:n race car:1:n racing car:1:n }\n{ roadster:1:n runabout:1:n two-seater:1:n }\n{ sedan:1:n saloon:3:n }\n{ sports car:1:n sport car:1:n }\n{ sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ Stanley Steamer:1:n }\n{ stock car:1:n }\n{ subcompact:1:n subcompact car:1:n }\n{ touring car:1:n phaeton:1:n tourer:2:n }\n{ used-car:1:n secondhand car:1:n }\n")

  // Q19
  @Test def findTransitiveHyponymsOfAnEnglishSynsetWithASpecifiedIdentifier() = result of "{}[id = `102958343` and lang=`en`].^hypernym+<#>" should equal ("{ ambulance:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ berlin:3:n }\n{ brougham:2:n }\n{ bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ compact:3:n compact car:1:n }\n{ convertible:1:n }\n{ coupe:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ electric:1:n electric automobile:1:n electric car:1:n }\n{ finisher:5:n }\n{ funny wagon:1:n }\n{ gas guzzler:1:n }\n{ gypsy cab:1:n }\n{ hardtop:1:n }\n{ hatchback:1:n }\n{ horseless carriage:1:n }\n{ hot rod:1:n hot-rod:1:n }\n{ jeep:1:n landrover:1:n }\n{ limousine:1:n limo:1:n }\n{ loaner:2:n }\n{ minicab:1:n }\n{ minicar:1:n }\n{ minivan:1:n }\n{ Model T:1:n }\n{ pace car:1:n }\n{ panda car:1:n }\n{ racer:2:n race car:1:n racing car:1:n }\n{ roadster:1:n runabout:1:n two-seater:1:n }\n{ sedan:1:n saloon:3:n }\n{ shooting brake:1:n }\n{ sports car:1:n sport car:1:n }\n{ sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ Stanley Steamer:1:n }\n{ stock car:2:n }\n{ stock car:1:n }\n{ subcompact:1:n subcompact car:1:n }\n{ touring car:1:n phaeton:1:n tourer:2:n }\n{ used-car:1:n secondhand car:1:n }\n")

  // Q20
  @Test def checkIfAnEnglishSynsetWithAGivenIdentifierIsABaseConcept() = result of "[{}[id = `100007846` and lang=`en`].bcs]" should equal ("true\n")

  // Q21
  @Test def checkIfAnEnglishSynsetWithAGivenIdentifierHasAGloss() = result of "[not empty({}[id = `100007846` and lang=`en`].desc)]" should equal ("true\n")

  // Q22

  @Test def findWordSensesAGlossAndAllEdgesOfSemanticRelationsOfAnEnglishSynsetWithAGivenIdentifier() = result of "{}[id = `100007846` and lang=`en`].senses|desc|_^dst&synset|dst^_^src&synset" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } desc a human being; \"there was too much for one person to do\"\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } member_holonym { people:1:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } ^partial_holonym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses individual:1:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses mortal:1:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses person:1:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses somebody:1:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses someone:1:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } senses soul:2:n\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } ^similar { person:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } similar { person:2:n }\n")

  //
  // Use Case II: Data Validation
  //

  // Q23
  @Test def findSynsetsThatContainTwoOrMoreSensesOfTheSameWord() = result of "{}[count(distinct(senses.word<#>)) < count(senses)]" should equal ("{ auto:4:n auto:5:n }\n")

  // Q24
  @Test def findSensesOfTheSameWordThatBelongToTheSameSynset() = result of "::$a.synset.senses$b[$a != $b and $a.word = $b.word]<$a, $b>" should equal ("auto:4:n auto:5:n\nauto:5:n auto:4:n\n")

  // Q25
  @Test def findSensesOfTheWordsThatBelongToTheGlossesOfTheirSynsets() = result of "::[word in flatten(string_split(synset.desc<#>, ` `))]" should equal ("area:5:n\ncar:1:n\ncar:4:n\ncar:5:n\ncold:1:a\ncold:2:a\nconstruction:4:n\nfixed:2:s\nflesh:2:n\nhatchback:1:n\norange:1:n\norange:1:s\norange:2:n\norange:4:n\npeople:1:n\nperson:1:n\nrigid:5:s\nset:2:n\nset:2:s\nstructure:1:n\n")

  // Q26
  @Test def findUsageExamplesWhichDoNotContainWordsWhatBelongToTheWordSensesOfTheSynsetsForWhichTheUsageExamplesHaveBeeCreated() = result of "{}$a.usage$b[empty($a.senses.word<#> intersect flatten(string_split($b, ` `)))]<$b>" should equal ("An invalid example of usage example.\n")

  // Q27
  @Test def findWordSensesThatHaveInvalidPOSSymbol() = result of "::[pos != `n` and pos != `v` and pos != `a` and pos != `s`]" should equal ("(no result)\n")

  // Q28
  @Test def findEnglishBaseConceptsThatDoNotHavePolishCounterparts() = result of "{}[lang = `en` and bcs and empty(ili[lang = `pl`])]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q29
  @Test def findEnglishBaseConceptsThatHavePolishCounterpartsThatAreNotBaseConcepts() = result of "{}[lang = `en` and bcs and not empty(ili[lang = `pl`]) and not (ili[lang = `pl`].bcs)]" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  // Q30
  @Test def findSynsetsThatDoNotHaveHypernymsAndAreNotBaseConcepts() = result of "{}[empty(hypernym) and not bcs]" should equal ("{ entity:1:n }\n{ organism:1:n being:2:n }\n{ great white shark:1:n white shark:1:n man-eater:2:n man-eating shark:1:n Carcharodon carcharias:1:n }\n{ homo:2:n man:4:n human being:1:n human:1:n }\n{ samochód:1:n auto:2:n }\n{ orange:2:n orangeness:1:n }\n{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n{ zymology:1:n zymurgy:1:n }\n{ grammatical category:1:n syntactic category:1:n }\n{ apple:1:n }\n{ orange:1:n }\n{ people:1:n }\n{ set:2:n }\n{ fleet:2:n }\n{ apple:2:n orchard apple tree:1:n Malus pumila:1:n }\n{ orange:3:n orange tree:1:n }\n{ zymosis:1:n zymolysis:1:n fermentation:2:n fermenting:1:n ferment:3:n }\n{ orange:4:n }\n{ determine:3:v set:2:v }\n{ play:1:v }\n{ orange:1:s orangish:1:s }\n{ cold:1:a }\n{ cold:2:a }\n{ fixed:2:s set:2:s rigid:5:s }\n{ zymotic:1:a zymolytic:1:a }\n{ non lexicalised sense:1:n }\n")

  // Q31
  @Test def findSynsetsThatFormHypernymyCycles() = result of "{}$a[$a in hypernym+]" should equal ("(no result)\n")

  // Q32
  @Test def findPolishSynsetsThatDoNotHaveGlosses() = result of "{}[lang = `pl` and empty(desc)]" should equal ("(no result)\n")

  // Q33
  @Test def findCrossPartOfSpeechHypernymyEdges() = result of "{}$a.hypernym$b[$a.senses.pos != $b.senses.pos]<$a, $b>" should equal ("{ auto:3:v } { auto:3:n }\n{ auto:3:v } { samochód:1:n auto:2:n }\n{ auto:4:n auto:5:n } { auto:3:v }\n{ wheel:1:n } { auto:3:v }\n")

  // Q34
  @Test def findRedundantHypernymyEdges() = result of "{}$b.hypernym$a[$a in $b.hypernym{2,}]<$a, $b>" should equal ("{ samochód:1:n auto:2:n } { auto:3:v }\n")

  // Q35
  @Test def findRedundantMeronymyEdges() = result of "{}$b.^partial_holonym$a[$a in $b.hypernym+.^partial_holonym]<$a, $b>" should equal ("{ wheel:1:n } { auto:4:n auto:5:n }\n")

  // Q36
  @Test def findGlossesAssignedToMoreThanOneSynset() = result of "{}.desc$a[count($a.^desc) > 1]<$a>" should equal ("Invalid synset introduced for the purpose of validation queries\na motor vehicle with four wheels; usually propelled by an internal combustion engine; \"he needs a car to get to work\"\n")

  // Q37
  @Test def findIdentifiersAssignedToMoreThanOneSense() = result of "{}.id$a[count($a.^id) > 1]<$a>" should equal ("(no result)\n")

  // Q38
  @Test def findSynsetsWithEmptyIdentifiers() = result of "{}[string_length(id<#>) = 0]" should equal ("(no result)\n")

  // Q39
  @Test def findWordsThatHaveNonConsecutiveSenseNumbers() = result of "''$w[not empty(senses$s.pos$p[$w.senses[pos = $p].sensenum != range(1, count($w.senses[pos = $p]))])]<$w>" should equal ("anatomy\narea\nauto\nbeing\nberlin\nbrougham\nbuild\nbus\ncab\ncause\ncompact\ncompartment\nconstruction\nconveyance\ndetermine\nferment\nfermentation\nfigure\nfinisher\nfixed\nfleet\nflesh\nform\nframe\ngondola\nhack\nheap\nhomo\ninstrumentality\nloaner\nmachine\nman\nman-eater\nphysique\nracer\nrigid\nsaloon\nset\nshape\nsoma\nsoul\ntourer\nunit\nwaggon\nwagon\nwhole\n")

  // Q40
  @Test def findEdgesOfSemanticRelationsThatLinkSynsetsThatTakePartInMoreThanOneSemanticRelation() = result of "{}$a._$b[issynset($b) and count($a._|^_.$b) > 1]" should equal ("$a={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } $b={ person:2:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } similar { person:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } del_this_rel { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b={ samochód:1:n auto:2:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ili { samochód:1:n auto:2:n }\n$a={ wheel:1:n } $b={ auto:3:v } { wheel:1:n } hypernym { auto:3:v }\n$a={ wheel:1:n } $b={ auto:3:v } { wheel:1:n } partial_holonym { auto:3:v }\n$a={ samochód:1:n auto:2:n } $b={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { samochód:1:n auto:2:n } ili { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n$a={ person:2:n } $b={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { person:2:n } similar { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  // Q41
  @Test def findSynsetsThatDoNotTakeParInAnySemanticRelation() = result of "{}[empty(_[issynset(#)]) and empty(^_[issynset(#)])]" should equal ("{ great white shark:1:n white shark:1:n man-eater:2:n man-eating shark:1:n Carcharodon carcharias:1:n }\n{ orange:2:n orangeness:1:n }\n{ zymology:1:n zymurgy:1:n }\n{ set:2:n }\n{ zymosis:1:n zymolysis:1:n fermentation:2:n fermenting:1:n ferment:3:n }\n{ orange:4:n }\n{ determine:3:v set:2:v }\n{ play:1:v }\n{ orange:1:s orangish:1:s }\n{ cold:1:a }\n{ cold:2:a }\n{ fixed:2:s set:2:s rigid:5:s }\n{ zymotic:1:a zymolytic:1:a }\n{ non lexicalised sense:1:n }\n")

  // Q42
  @Test def findPolishSynsetsSuchThatTheirEnglishCounterpartsHaveHypernymsThatAreNotCounterpartsOfThePolishSynsetsHypernyms() = result of "{}$a[lang = `pl` and not empty(ili[lang = `en` and not hypernym in $a.hypernym.ili])]<$a>" should equal ("{ samochód:1:n auto:2:n }\n")

  // Q43
  @Test def findHyponymHypernymPairsThatContainSensesOfTheSameWord() = result of "{}$a.hypernym$b[count($a.senses.word<#> intersect senses.word<#>) > 0]<$a, $b>" should equal ("{ auto:3:n } { samochód:1:n auto:2:n }\n{ auto:3:v } { auto:3:n }\n{ auto:3:v } { samochód:1:n auto:2:n }\n{ auto:4:n auto:5:n } { auto:3:v }\n{ auto:6:n } { auto:4:n auto:5:n }\n")

  // Q44
  @Test def findPairsOfSynsetsTharParticipateBothInHypernymyAndMeronymy() = result of "{}$a.hypernym$b[$a in $b.^partial_holonym]<$a, $b>" should equal ("{ wheel:1:n } { auto:3:v }\n")

  // Q45
  @Test def findPairsOfNearAntonymsThatDoNotHaveCommonHypernym() = result of "{}$a.near_antonym$b[$a.hypernym != $b.hypernym]<$a, $b>" should equal ("{ auto:6:n } { wheel:1:n }\n")

  // Q46
  @Test def findSynsetsThatHaveMoreThanOneHypernym() = result of "{}[count(hypernym) > 1]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ auto:3:v }\n{ minicab:1:n }\n{ wheeled vehicle:1:n }\n")

  // Q47: find words that have more than k word senses
  // Q47a: k = 3
  @Test def findWordsThatHaveMoreThan3Senses() = result of "''[count(senses) > 3]" should equal ("auto\ncar\norange\n")

  // Q48
  @Test def findWordsThatBelongToNonLexicalizedSynsets() = result of "''[senses.synset.nl]" should equal ("non lexicalised sense\n")

  // Q49: find text data that contain characters that do not belong to a specified set
  @Test def findTextDataThatContainCharactersThatDoNotBelongToEnglishAlphabet() = result of "__$a[$a =~ `[^a-zA-Z0-9 ()-;,\"'\n\t?]`]<$a>" should equal ("samochód\n")

  // Q50
  @Test def findPolishSynsetsThatDoNotHaveHypernyms() = result of "{}[lang = `pl` and empty(hypernym)]" should equal ("{ samochód:1:n auto:2:n }\n")

  // Q51
  @Test def findLongestPathsInHypernymyHierarchy() = result of "longest({}.hypernym+)" should equal ("{ berlin:3:n } hypernym { limousine:1:n limo:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ brougham:2:n } hypernym { sedan:1:n saloon:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ finisher:5:n } hypernym { racer:2:n race car:1:n racing car:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ funny wagon:1:n } hypernym { ambulance:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ gypsy cab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ minicab:1:n } hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ minicab:1:n } hypernym { minicar:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ panda car:1:n } hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ shooting brake:1:n } hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ stock car:2:n } hypernym { racer:2:n race car:1:n racing car:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  // Q52
  @Test def findEnglishBaseConcepts() = result of "{}[bcs and lang = `en`]" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  //
  // Use Case III: Data Aggregation
  //

  // Q53: Find how many words/synsets/word senses/glosses there are in the wordnet.
  // Q53.1: words
  @Test def countWords() = result of "count('')" should equal ("175\n")
  // Q53.2: synsets
  @Test def countSynsets() = result of "count({})" should equal ("99\n")
  // Q53.3: word senses
  @Test def countWordSenses() = result of "count(::)" should equal ("200\n")
  // Q53.4: glosses
  @Test def countGlosses() = result of "count(desc)" should equal ("99\n")

  // Q53a: Find how many nouns/noun synsets/noun word senses/noun glosses there are in the wordnet.
  // Q53a.1: words
  @Test def countNouns() = result of "count(''[`n` in senses.pos])" should equal ("167\n")
  // Q53a.2: synsets
  @Test def countNounSynsets() = result of "count({}[senses.pos = `n`])" should equal ("91\n")
  // Q53a.3: word senses
  @Test def countNounWordSenses() = result of "count(::[pos = `n`])" should equal ("187\n")
  // Q53a.4: glosses
  @Test def countNounGlosses() = result of "count({}[senses.pos = `n`].desc)" should equal ("91\n")

  // Q54
  @Test def countSemanticRelationEdges() = result of "count({}._[issynset(#)])" should equal ("92\n")

  // Q54a
    @Test def countEdgesOfSemanticRelationsThatHoldBetweenNounSynsets() = result of "count({}$a._^dst&synset$b[$a.senses.pos = `n` and $b.senses.pos = `n`])" should equal ("87\n")

  // Q55
  @Test def countHypernymyEdges() = result of "count({}.hypernym)" should equal ("77\n")

  // Q55a
  @Test def countHypernymyEdgesThatHoldBetweenNouns() = result of "count({}$a.hypernym$b[$a.senses.pos = `n` and $b.senses.pos = `n`])" should equal ("73\n")

  // Q56
  @Test def countTheNumberOfEdgesPerSynset() = result of "count({}._[issynset(#)])/count({})" should startWith ("0.929")

  // Q56a
  @Test def countTheNumberOfEdgesPerNounSynset() = result of "count({}[pos = `n`]._[issynset(#)])/count({}[pos = `n`])" should startWith ("0.989")

  // Q57: count the number of senses per synset/word
  // Q57.1: synset
  @Test def countTheNumberOfSensesPerSynset() = result of "count(::)/count({})" should startWith ("2.020")
  // Q57.2: word
  @Test def countTheNumberOfSensesPerWord() = result of "count(::)/count('')" should startWith ("1.142")

  // Q57a: count the number of senses per noun synset/noun
  // Q57a.1: synset
  @Test def countTheNumberOfSensesPerNounSynset() = result of "count(::[pos = `n`])/count({}[senses.pos = `n`])" should startWith ("2.054")
  // Q57a.2: word
  @Test def countTheNumberOfSensesPerNoun() = result of "count(::[pos = `n`])/count(''[`n` in senses.pos])" should startWith ("1.1197")

  // Q58
  @Test def countTheNumberOfSynsetsThatDoNotHaveHypernyms() = result of "count({}[empty(hypernym)])" should equal ("26\n")

  // Q59
  @Test def countTheNumberOfSemanticRelationsSuccessorsPerNonLeafSynset() = result of "count({}[not empty(^hypernym)]._[issynset(#)])/count({}[not empty(^hypernym)])" should startWith ("1.176")

  // Q60
  @Test def countTheNumberOfSynsetsPerHypernymyTree() = result of "count({})/count({}[empty(hypernym)])" should startWith ("3.807")

  // Q61
  @Test def countTheNumberOfSynsetsThatHaveMultipleHypernyms() = result of "count({}[count(hypernym)>1])" should equal ("4\n")

  // Q62: Count how many synsets that have multiple hypernyms are at n-th level of hypernymy hierarchy
  // Q62.1: n = 3
  @Test def countTheNumberOfSynsetsThatHaveMultipleHypernymsAtTheThirdLevelOfHypernymyHierarchy() = result of "count({}[count(hypernym)>1 and 3 in size(hypernym+[empty(hypernym)])])" should equal ("1\n")

  // Q63
  @Test def countHowManySynsetsThatHaveMultipleHypernymsInheritFromMoreThanOneHypernymyTree() = result of "count({}[count(hypernym)>1 and count(distinct(hypernym+[empty(hypernym)]<#>))>1])" should equal ("1\n")

  // Q64: How many leaves/roots/inner nodes there are in the hypernymy hierarchy?
  // Q64.1: leaves
  @Test def countLeavesOfHypernymy() = result of "count({}[empty(^hypernym)])" should equal ("65\n")
  // Q64.1: roots
  @Test def countRootsOfHypernymy() = result of "count({}[empty(hypernym)])" should equal ("26\n")
  // Q64.1: inner nodes
  @Test def countInnerNodesOfHypernymy() = result of "count({}[not empty(^hypernym) and not empty(hypernym)])" should equal ("29\n")

  // Q65
  @Test def countTransitiveHyponymsOfASynsetThatContainsAGivenSense() = result of "count(distinct({car:1:n}.^hypernym+<#>))" should equal ("40\n")

  // Q66
  @Test def countSynsetsThatDoNotTakePartInHypernymy() = result of "count({}[empty(hypernym) and empty(^hypernym)])" should equal ("21\n")

  // Q67
  @Test def countEnglishBaseConceptsThatHaveCounterpartsInPolish() = result of "count({}[bcs and lang = `en`].ili[lang = `pl`])" should equal ("1\n")

  // Q68: What is the minimal/maximal distance from the synset that contains a given word sense to the root of hypernymy?
  // Q68.1: minimal
  @Test def findMinimalDistanceFromTheSynsetThatContainsAGivenSenseToTheRootOfHypernymyTree() = result of "size(shortest(({car:1:n}.hypernym*)$a[empty($a.hypernym)]))" should equal ("11\n")
  // Q68.2: maximal
  @Test def findMaximalDistanceFromTheSynsetThatContainsAGivenSenseToTheRootOfHypernymyTree() = result of "size(longest(({car:1:n}.hypernym*)$a[empty($a.hypernym)]))" should equal ("12\n")

  // Q69: What is the minimal/maximal distance from the synset that contains a given word sense to the leaves of hypernymy?
  // Q69.1: minimal
    @Test def findMinimalDistanceFromTheSynsetThatContainsAGivenSenseToTheLeavesOfHypernymyTree() = result of "size(shortest(({car:1:n}.^hypernym*)$a[empty($a.^hypernym)]))" should equal ("2\n")
  // Q69.2: maximal
    @Test def findMaximalDistanceFromTheSynsetThatContainsAGivenSenseToTheLeavesOfHypernymyTree() = result of "size(longest(({car:1:n}.^hypernym*)$a[empty($a.^hypernym)]))" should equal ("3\n")

  // Q70
    @Test def determineClusteringCoefficientForTheSynsetThatContainsAGivenSense() = result of "(count(({auto:2:n}._^dst&synset|dst^_^src&synset<#>)._$a[$a in {auto:2:n}._^dst&synset|dst^_^src&synset])$kna,count({auto:2:n}._^dst&synset|dst^_^src&synset)$ka)<$kna/($ka*($ka - 1))>" should startWith ("0.083")

  // Q71: How many paths of a given length can be constructed from arbitrary chosen semantic relations
  // Q71.1: 3-elemnt paths build from hyponyms followed by meronyms meronyms
  @Test def countThreeElementPathsBuildFromHyponymsFollowedByMeronyms() = result of "range(1, max(size({}.^hypernym+.^partial_holonym)))$i<$i, count(({}.^hypernym+.^partial_holonym)@P[size(@P) = $i])>" should equal ("1 0\n2 0\n3 6\n4 5\n5 3\n6 1\n7 1\n8 1\n")

  // Q72: Count synsets/words by the number of senses
  // Q72.1: synsets
  @Test def countSynsetsByTheNumberOfSenses() = result of "range(1, (max({}$a<count($a.senses)>)))$b<$b, count({}[count(senses) = $b])>" should equal ("1 48\n2 32\n3 8\n4 4\n5 3\n6 2\n7 1\n8 0\n9 0\n10 0\n11 0\n12 0\n13 0\n14 1\n")
  // Q72.2: words
  @Test def countWordsByTheNumberOfSenses() = result of "range(1, (max(''$a<count($a.senses)>)))$b<$b, count(''[count(senses) = $b])>" should equal ("1 165\n2 3\n3 4\n4 0\n5 2\n6 0\n7 1\n")

  //
  // Use Case IV: Determining the similarity of concepts
  //

  // Q73
  @Test def findTheSizeOfTheLongestPathThatLinksHypernymyRootWithALeaf() = result of "max(size(longest({}[empty(^hypernym)].hypernym*)))" should equal ("14\n")

  // Q74
  @Test def findShortestPathThatLinksTwoSynsetsContainingArbitrarySensesByTheEdgesOfHypernymyFollowedByHyponymy() = result of "min(size({minicab:1:n}.hypernym*.^hypernym*.{ambulance:1:n}))" should equal ("4\n")

  // Q75: Find 2-5 element paths that connect synsets contains given words through the edges of "up" relations followed by "down" relationss
  // Q75.1: up = hyponymy, down = partial and member meronymy
  @Test def find2To5ElemntPathsThatLinksTwoSynsetsContainingArbitraryWordsByTheEdgesOfHypernymyFollowedByPartialOrMemberMeronymy() = result of "({auto:2:n}.^hypernym{1,5}.(^partial_holonym|^member_holonym){1,5})@P[size(@P) <= 5]<@P>" should equal ("{ samochód:1:n auto:2:n } ^hypernym { auto:3:n } ^hypernym { auto:3:v } ^hypernym { auto:4:n auto:5:n } ^partial_holonym { wheel:1:n }\n{ samochód:1:n auto:2:n } ^hypernym { auto:3:n } ^hypernym { auto:3:v } ^partial_holonym { wheel:1:n }\n{ samochód:1:n auto:2:n } ^hypernym { auto:3:v } ^hypernym { auto:4:n auto:5:n } ^partial_holonym { wheel:1:n }\n{ samochód:1:n auto:2:n } ^hypernym { auto:3:v } ^partial_holonym { wheel:1:n }\n")

  // Q76
  @Test def findPathsThatLinkSynsetsContainingGivenWordToSynsetsThatContainAnotherWordOrItsHypernymOrHyponym() = result of "{taxicab}._&synset*.{car}.(hypernym|^hypernym)?" should equal ("{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n")

  // Q77
  @Test def findTheMostSpecificGeneralizationOfSynsetsContainingGivenWordSenses() = result of "maxby(({minicab:1:n}.hypernym*)$a.^hypernym*.{ambulance:1:n}<$a,size($a.hypernym*)>, 2)$a@_<$a>" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  //
  // Use Case V: Checking word sense membership
  //

  // Q78
  @Test def checkIfTwoSensesBelongToTheSameSynset() = result of "[{car:1:n} = {auto:1:n}]" should equal ("true\n")

  // Q79
  @Test def checkIfAGivenSenseBelongsToTheHyponymOfASynsetThatContainsAnotherSense() = result of "[{taxicab:1:n} in {car:1:n}.^hypernym]" should equal ("true\n")

  // Q80
  @Test def checkIfAGivenSenseBelongsToTransitiveHyponymOfASynsetThatContainsAnotherSense() = result of "[{minicab:1:n} in {car:1:n}.^hypernym+]" should equal ("true\n")

  // Q81: Find a disjunction of some of the queries from Q78-Q80
  // Q81.1: Find a disjunction of Q78 and Q80
  @Test def checkIfAGivenSenseBelongsToHyponymOfASynsetThatContainsAnotherSenseOrToThatSynset() = result of "[[{auto:1:n} in {car:1:n}.^hypernym] or [{auto:1:n} = {car:1:n}]]" should equal ("true\n")

  // Q82
  @Test def testRemoveRedundantHypernymyLinks() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{}$b.hypernym$a[$a in $b.hypernym{2,}]<$a,$b>" should equal ("{ samochód:1:n auto:2:n } { auto:3:v }\n")
    result(wupdate) of "from {}$b.hypernym$a[$a in $b.hypernym{2,}] update $b hypernym -= $a" should equal ("(no result)\n")
    result(wupdate) of "{}$b.hypernym$a[$a in $b.hypernym{2,}]<$a,$b>" should equal ("(no result)\n")
  }

  // Q83
  @Test def testRemoveRedundantMeronymyLinks() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{}$b.^partial_holonym$a[$a in $b.hypernym+.^partial_holonym]<$a,$b>" should equal ("{ wheel:1:n } { auto:4:n auto:5:n }\n")
    result(wupdate) of "from {}$b.^partial_holonym$a[$a in $b.hypernym+.^partial_holonym] update $b dst^partial_holonym^src -= $a" should equal ("(no result)\n")
    result(wupdate) of "{}$b.^partial_holonym$a[$a in $b.hypernym+.^partial_holonym]<$a,$b>" should equal ("(no result)\n")
  }

  // Q84
  @Test def renumerateSensesOfWordsThatHaveNonConsecutiveSenseNumbers() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "''$w[not empty(senses$s.pos$p[$w.senses[pos = $p].sensenum != range(1, count($w.senses[pos = $p]))])]<$w>" should equal ("anatomy\narea\nauto\nbeing\nberlin\nbrougham\nbuild\nbus\ncab\ncause\ncompact\ncompartment\nconstruction\nconveyance\ndetermine\nferment\nfermentation\nfigure\nfinisher\nfixed\nfleet\nflesh\nform\nframe\ngondola\nhack\nheap\nhomo\ninstrumentality\nloaner\nmachine\nman\nman-eater\nphysique\nracer\nrigid\nsaloon\nset\nshape\nsoma\nsoul\ntourer\nunit\nwaggon\nwagon\nwhole\n")
    result(wupdate) of
      """from ''$w[not empty(senses$s.pos$p[$w.senses[pos = $p].sensenum != range(1, count($w.senses[pos = $p]))])] do
        |  from $w.senses.pos$p do
        |    %z := 1
        |    from $w.senses$b[pos = $p] do
        |      update $b sensenum := %z
        |      %z := %z + 1
        |    end
        |  end
        |end
      """.stripMargin should equal ("(no result)\n")
    result(wupdate) of "''$w[not empty(senses$s.pos$p[$w.senses[pos = $p].sensenum != range(1, count($w.senses[pos = $p]))])]<$w>" should equal ("(no result)\n")
  }

  // Q85
  @Test def testMergeSynsetsAndSenses() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "merge {car} union {minicab:1:n} union 'gypsy cab':1:n" should equal ("(no result)\n")
    result(wupdate) of "{minicab:1:n}" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n car:2:n railcar:1:n railway car:1:n railroad car:1:n car:3:n gondola:3:n car:4:n elevator car:1:n cable car:1:n car:5:n minicab:1:n gypsy cab:1:n }\n")
  }

  // Q86
  @Test def testMergeSynsetsOfTheSameWordThatContainMoreThanOneWordSense() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{car:1:n}" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
    result(wupdate) of "from {}$a[count(senses) > 1] merge {$a.senses.word}[not empty(senses.word<#> intersect $a.senses.word<#>) and count(senses) > 1]" should equal ("(no result)\n")
    result(wupdate) of "{car:1:n}" should equal ("{ samochód:1:n auto:2:n auto:4:n auto:5:n cable car:1:n car:5:n car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n car:2:n railcar:1:n railway car:1:n railroad car:1:n car:3:n gondola:3:n car:4:n elevator car:1:n }\n")
  }

  // Q87
  @Test def testMergeSynsetsOfTheSameWordThatShareAHypernym() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{car:3:n}" should equal ("{ car:3:n gondola:3:n }\n")
    result(wupdate) of "from {}$a merge {$a.senses.word}[not empty(senses.word<#> intersect $a.senses.word<#>) and not empty(hypernym<#> intersect $a.hypernym<#>)]" should equal ("(no result)\n")
    result(wupdate) of "{car:3:n}" should equal ("{ cable car:1:n car:5:n car:3:n gondola:3:n car:4:n elevator car:1:n }\n")
  }

  // Q88: Merge synsets that share at least k words.
  // Q88.1 k = 2
  @Test def testMergeSynsetsThatShareAtLeastTwoWords() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{generated}" should equal ("{ generated:1:n prepared:1:n }\n{ generated:2:n prepared:2:n }\n{ generated:3:n prepared:3:n }\n")
    result(wupdate) of "from {}$a merge {$a.senses.word}[count(senses.word<#> intersect $a.senses.word<#>) > 1]" should equal ("(no result)\n")
    result(wupdate) of "{generated}" should equal ("{ generated:1:n prepared:1:n generated:2:n prepared:2:n generated:3:n prepared:3:n }\n")
  }

  // Q89
  @Test def testMergeSynsetsOfTheSameWordThatShareAnAntonym() = {
    val wupdate = WQueryTestSuiteRuntime.newWUpdate

    result(wupdate) of "{generated}" should equal ("{ generated:1:n prepared:1:n }\n{ generated:2:n prepared:2:n }\n{ generated:3:n prepared:3:n }\n")
    result(wupdate) of "from ''$a.senses$b.antonym$c merge $a.senses[$c in antonym].synset" should equal ("(no result)\n")
    result(wupdate) of "{generated}" should equal ("{ generated:1:n prepared:1:n }\n{ generated:2:n prepared:2:n generated:3:n prepared:3:n }\n")
  }

}
