package org.wquery
import org.testng.annotations.Test

class FiltersTestSuite extends WQueryTestSuite {
  //  
  // comparisons
  //
      
  @Test def testBooleanLowerThanTrue() = result of ("[false < true]") should equal ("true\n")
    
  @Test def testBooleanLowerThanFalse() = result of ("[true < false]") should equal ("false\n")
    
  @Test def testIntLowerThanTrue() = result of ("[1 < 2]") should equal ("true\n")
    
  @Test def testIntDoubleLowerThanTrue() = result of ("[1 < 1.2]") should equal ("true\n")
    
  @Test def testDoubleLowerThanTrue() = result of ("[1.1 < 1.2]") should equal ("true\n")
    
  @Test def testIntLowerThanFalse() = result of ("[2 < 1]") should equal ("false\n")
    
  @Test def testIntDoubleLowerThanFalse() = result of ("[1.2 < 1]") should equal ("false\n")
    
  @Test def testDoubleLowerThanFalse() = result of ("[1.2 < 1.1]") should equal ("false\n")
    
  @Test def testStringLowerThanTrue() = result of ("[car < person]") should equal ("true\n")
    
  @Test def testStringLowerThanFalse() = result of ("[person < car]") should equal ("false\n")
    
  @Test def testSenseLowerThanTrue() = result of ("[person:1:n < car:1:n]") should equal ("false\n")
    
  @Test def testSenseLowerThanFalse() = result of ("[car:1:n < person:1:n]") should equal ("true\n")
    
  @Test def testSynsetLowerThanTrue() = result of ("[{person:1:n} < {person:3:n}]") should equal ("true\n")
    
  @Test def testSynsetLowerThanFalse() = result of ("[{person:3:n} < {person:1:n}]") should equal ("false\n")    

  @Test def testBooleanGreaterThanTrue() = result of ("[true > false]") should equal ("true\n")
    
  @Test def testBooleanGreaterThanFalse() = result of ("[false > true]") should equal ("false\n")
    
  @Test def testIntGreaterThanTrue() = result of ("[2 > 1]") should equal ("true\n")
    
  @Test def testIntDoubleGreaterThanTrue() = result of ("[2 > 1.2]") should equal ("true\n")
    
  @Test def testDoubleGreaterThanTrue() = result of ("[1.2 > 1.1]") should equal ("true\n")
    
  @Test def testIntGreaterThanFalse() = result of ("[1 > 2]") should equal ("false\n")
    
  @Test def testIntDoubleGreaterThanFalse() = result of ("[1.2 > 2]") should equal ("false\n")
    
  @Test def testDoubleGreaterThanFalse() = result of ("[1.1 > 1.2]") should equal ("false\n")
    
  @Test def testStringGreaterThanTrue() = result of ("[person > car]") should equal ("true\n")
    
  @Test def testStringGreaterThanFalse() = result of ("[car > person]") should equal ("false\n")
    
  @Test def testSenseGreaterThanTrue() = result of ("[car:1:n > person:1:n]") should equal ("false\n")
    
  @Test def testSenseGreaterThanFalse() = result of ("[person:1:n > car:1:n]") should equal ("true\n")
    
  @Test def testSynsetGreaterThanTrue() = result of ("[{person:3:n} > {person:1:n}]") should equal ("true\n")
    
  @Test def testSynsetGreaterThanFalse() = result of ("[{person:1:n} > {person:3:n}]") should equal ("false\n")
    
  @Test def testRegexMatchesTrue() = result of ("[person.senses.word =~ `^per`]") should equal ("true\n")
    
  @Test def testRegexMatchesFalse() = result of ("[{person}.words =~ `^per`]") should equal ("false\n")    
    
  // <= 
    
  // >=
  @Test def testInequalityTrue() = result of ("[{person:1:n}.pos = {person:2:n}.pos]") should equal ("true\n")

  @Test def testInequalityFalse() = result of ("[{person:1:n}.pos != {person:2:n}.pos]") should equal ("false\n")

  @Test def testSenseInSensesTrue() = result of ("[individual:1:n in {person}.senses]") should equal ("true\n")    
    
  @Test def testSenseInSensesFalse() = result of ("[individual:1:n in {person:3:n}.senses]") should equal ("false\n")
    
  @Test def testSensePinSensesTrue() = result of ("[{individual:1:n}.senses pin {person}.senses]") should equal ("true\n")
    
  @Test def testSensePinSensesFalse() = result of ("[{individual:1:n}.senses pin {person:1:n}.senses]") should equal ("false\n")
    
  // comparison exceptions -- after exception refactoring  
  
  //
  // logical operators
  //  
    
  @Test def testAndTrue() = result of ("[1 < 2 and 2 < 3]") should equal ("true\n")
    
  @Test def testAndFalse() = result of ("[1 < 2 and 3 < 3]") should equal ("false\n")
    
  @Test def testOrTrue() = result of ("[1 < 2 or 3 < 3]") should equal ("true\n")
    
  @Test def testOrFalse() = result of ("[1 < 1 or 4 < 3]") should equal ("false\n")
    
  @Test def testNotTrue() = result of ("[not 3 < 3]") should equal ("true\n")
    
  @Test def testNotFalse() = result of ("[not 1 < 2]") should equal ("false\n")
    
  // test or and not () precedence    

  // [# < 2]
  
  @Test def testSingleBackReference() = result of ("last({person:1}.senses[# != individual:1:n])") should equal ("mortal:1:n\nperson:1:n\nsomebody:1:n\nsomeone:1:n\nsoul:2:n\n")
    
  @Test def testRelationAfterSingleBackReference() = result of ("last({person:1}.senses[# != individual:1:n].word)") should equal ("mortal\nperson\nsomebody\nsomeone\nsoul\n")
    
  @Test def testTwoSingleBackReferences() = result of ("last({person}.senses[# != person:2:n].word[# != mortal])") should equal ("individual\nperson\nsomebody\nsomeone\nsoul\n")    
    
  @Test def testSingleAndTripleBackReference() = result of ("last({person}.senses.word[### != individual:1:n and # != mortal])") should equal ("person\nsomebody\nsomeone\nsoul\n")

  @Test def testSingleBackReferenceDotHypernyms() = result of ("{person}[{organism:1:n} in #.hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReference() = result of ("{person}[{organism:1:n} in hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
 
  @Test def testSingleBackReferenceDotHypernymsWords() = result of ("{person}[organism in #.hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReferenceDotHypernymsWords() = result of ("{person}[organism in hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def testSingleBackReferenceDotHypernymsCount() = result of ("{person}[count(#.hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsCount() = result of ("{person}[count(hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsOrHyponymsWords() = result of ("{person}[organism in (hypernym|^hypernym).words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
  
  @Test def testArcFilter() = result of ("{}.partial_holonym|member_holonym$r$_[$r = \\member_holonym]") should equal ("$r=member_holonym { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } member_holonym { people:1:n }\n$r=member_holonym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } member_holonym { fleet:2:n }\n")

  // generator filters

  @Test def testSynsetsByWordGeneratorFilter()  = result of ("{bus}.hypernym.{car}") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testSynsetBySenseGeneratorFilter()  = result of ("{bus}.hypernym.{car:1:n}") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testQuotedWordGeneratorFilter()  = result of ("{bus}.hypernym.words.'car'") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } words car\n")

  @Test def testUnquotedWordGeneratorFilter()  = result of ("{bus}.hypernym.words.car") should startWith ("ERROR: Arc expression car references an unknown relation or argument")

  @Test def testQuotedSenseGeneratorFilter()  = result of ("{bus}.hypernym.senses.'car':1:n") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } senses car:1:n\n")

  @Test def testParenthesedSenseGeneratorFilter()  = result of ("{bus}.hypernym.senses.(car:1:n)") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } senses car:1:n\n")

  @Test def testIntegerGeneratorFilter()  = result of ("{bus}.senses.sensenum.3") should equal ("{ bus:4:n jalopy:1:n heap:3:n } senses heap:3:n sensenum 3\n")

  @Test def testSequenceGeneratorFilter()  = result of ("{bus}.senses.sensenum.3..8") should equal ("{ bus:4:n jalopy:1:n heap:3:n } senses bus:4:n sensenum 4\n{ bus:4:n jalopy:1:n heap:3:n } senses heap:3:n sensenum 3\n")

  @Test def testFloatGeneratorFilter()  = result of ("{bus}.senses.sensenum.(3.0)") should equal ("{ bus:4:n jalopy:1:n heap:3:n } senses heap:3:n sensenum 3\n")

  @Test def testSingleBackReferenceGeneratorFilter()  = result of ("{bus}.hypernym.#") should equal ("{ bus:4:n jalopy:1:n heap:3:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testMultiBackReferenceGeneratorFilter()  = result of ("{bus}.hypernym.###") should equal ("(no result)\n")

  @Test def testFilterGeneratorFilter()  = result of ("{bus}.nl.[1!=1]") should equal ("{ bus:4:n jalopy:1:n heap:3:n } nl false\n")

  @Test def testVariableGeneratorFilter()  = result of ("{bus}$a.hypernym.$a") should equal ("(no result)\n")

  @Test def testArcGeneratorFilter()  = result of ("{bus}.hypernym$a$_<$a>.\\hypernym") should equal ("hypernym\n")

  @Test def testFunctionGeneratorFilter()  = result of ("{bus}.senses.sensenum.(min(1..10))") should equal ("{ bus:4:n jalopy:1:n heap:3:n } senses jalopy:1:n sensenum 1\n")

  @Test def testBooleanGeneratorFilter()  = result of ("{car}.nl.[false]") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } nl false\n")

  // boolean path filters

  @Test def testBooleanPathFilterIsTrue()  = result of ("{}[nl]") should equal ("{ non lexicalised sense:1:n }\n")

  @Test def testBooleanPathFilterIsFalse()  = result of ("{person}[not nl]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")

  @Test def testMultisetEqualityTrue()  = result of ("[car union car = car union car]") should equal ("true\n")

  @Test def testMultisetEqualityFalse()  = result of ("[car = car union car]") should equal ("false\n")

  @Test def testMultisetInequalityTrue()  = result of ("[car != car union car]") should equal ("true\n")

  @Test def testMultisetInequalityFalse()  = result of ("[car union car != car union car]") should equal ("false\n")

}