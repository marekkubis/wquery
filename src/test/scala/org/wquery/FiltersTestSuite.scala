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
    
  @Test def testSenseLowerThanTrue() = result of ("[person:1:n < car:1:n]") should equal ("true\n")
    
  @Test def testSenseLowerThanFalse() = result of ("[car:1:n < person:1:n]") should equal ("false\n")        
    
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
    
  @Test def testSenseGreaterThanTrue() = result of ("[car:1:n > person:1:n]") should equal ("true\n")
    
  @Test def testSenseGreaterThanFalse() = result of ("[person:1:n > car:1:n]") should equal ("false\n")
    
  @Test def testSynsetGreaterThanTrue() = result of ("[{person:3:n} > {person:1:n}]") should equal ("true\n")
    
  @Test def testSynsetGreaterThanFalse() = result of ("[{person:1:n} > {person:3:n}]") should equal ("false\n")
    
  @Test def testRegexMatchesTrue() = result of ("[person.senses.word =~ `^per`]") should equal ("true\n")
    
  @Test def testRegexMatchesFalse() = result of ("[{person}.words =~ `^per`]") should equal ("false\n")    
    
  // <= 
    
  // >= 
    
  // = 
    
  // !=

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
  
  //
  // in path checks NOW
  //
      
  // [# < 2]
  @Test def testSingleBackReference() = result of ("last({person:1}.senses[# != individual:1:n])") should equal ("person:1:n\nsomeone:1:n\nsomebody:1:n\nmortal:1:n\nsoul:2:n\n")
    
  @Test def testRelationAfterSingleBackReference() = result of ("last({person:1}.senses[# != individual:1:n].word)") should equal ("mortal\nperson\nsomebody\nsomeone\nsoul\n")
    
  @Test def testTwoSingleBackReferences() = result of ("last({person}.senses[# != person:2:n].word[# != mortal])") should equal ("individual\nperson\nsomebody\nsomeone\nsoul\n")    
    
  @Test def testSingleAndTripleBackReference() = result of ("last({person}.senses.word[### != individual:1:n and # != mortal])") should equal ("person\nsomebody\nsomeone\nsoul\n")

  @Test def testSingleBackReferenceDotHypernyms() = result of ("{person}[{organism:1:n} in #.hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReference() = result of ("{person}[{organism:1:n} in hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
 
  @Test def testSingleBackReferenceDotHypernymsWords() = result of ("{person}[organism in #.hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReferenceDotHypernymsWords() = result of ("{person}[organism in hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def testSingleBackReferenceDotHypernymsCount() = result of ("{person}[count(#.hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsCount() = result of ("{person}[count(hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsOrHyponymsWords() = result of ("{person}[organism in hypernym|^hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
}