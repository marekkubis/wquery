package org.wquery

import org.testng.annotations.Test

class FiltersTestSuite extends WQueryTestSuite {
  //  
  // comparisons
  //
      
  @Test def testBooleanLowerThanTrue() = resultOf("[false < true]") should equal ("true\n")
    
  @Test def testBooleanLowerThanFalse() = resultOf("[true < false]") should equal ("false\n")
    
  @Test def testIntLowerThanTrue() = resultOf("[1 < 2]") should equal ("true\n")
    
  @Test def testIntDoubleLowerThanTrue() = resultOf("[1 < 1.2]") should equal ("true\n")
    
  @Test def testDoubleLowerThanTrue() = resultOf("[1.1 < 1.2]") should equal ("true\n")
    
  @Test def testIntLowerThanFalse() = resultOf("[2 < 1]") should equal ("false\n")
    
  @Test def testIntDoubleLowerThanFalse() = resultOf("[1.2 < 1]") should equal ("false\n")
    
  @Test def testDoubleLowerThanFalse() = resultOf("[1.2 < 1.1]") should equal ("false\n")
    
  @Test def testStringLowerThanTrue() = resultOf("[car < person]") should equal ("true\n")
    
  @Test def testStringLowerThanFalse() = resultOf("[person < car]") should equal ("false\n")
    
  @Test def testSenseLowerThanTrue() = resultOf("[person:1:n < car:1:n]") should equal ("true\n")
    
  @Test def testSenseLowerThanFalse() = resultOf("[car:1:n < person:1:n]") should equal ("false\n")        
    
  @Test def testSynsetLowerThanTrue() = resultOf("[{person:1:n} < {person:3:n}]") should equal ("true\n")
    
  @Test def testSynsetLowerThanFalse() = resultOf("[{person:3:n} < {person:1:n}]") should equal ("false\n")    

  @Test def testBooleanGreaterThanTrue() = resultOf("[true > false]") should equal ("true\n")
    
  @Test def testBooleanGreaterThanFalse() = resultOf("[false > true]") should equal ("false\n")
    
  @Test def testIntGreaterThanTrue() = resultOf("[2 > 1]") should equal ("true\n")
    
  @Test def testIntDoubleGreaterThanTrue() = resultOf("[2 > 1.2]") should equal ("true\n")
    
  @Test def testDoubleGreaterThanTrue() = resultOf("[1.2 > 1.1]") should equal ("true\n")
    
  @Test def testIntGreaterThanFalse() = resultOf("[1 > 2]") should equal ("false\n")
    
  @Test def testIntDoubleGreaterThanFalse() = resultOf("[1.2 > 2]") should equal ("false\n")
    
  @Test def testDoubleGreaterThanFalse() = resultOf("[1.1 > 1.2]") should equal ("false\n")
    
  @Test def testStringGreaterThanTrue() = resultOf("[person > car]") should equal ("true\n")
    
  @Test def testStringGreaterThanFalse() = resultOf("[car > person]") should equal ("false\n")
    
  @Test def testSenseGreaterThanTrue() = resultOf("[car:1:n > person:1:n]") should equal ("true\n")
    
  @Test def testSenseGreaterThanFalse() = resultOf("[person:1:n > car:1:n]") should equal ("false\n")
    
  @Test def testSynsetGreaterThanTrue() = resultOf("[{person:3:n} > {person:1:n}]") should equal ("true\n")
    
  @Test def testSynsetGreaterThanFalse() = resultOf("[{person:1:n} > {person:3:n}]") should equal ("false\n")
    
  @Test def testRegexMatchesTrue() = resultOf("[person.senses.word =~ `^per`]") should equal ("true\n")
    
  @Test def testRegexMatchesFalse() = resultOf("[{person}.words =~ `^per`]") should equal ("false\n")    
    
  // <= 
    
  // >= 
    
  // = 
    
  // !=

  @Test def testSenseInSensesTrue() = resultOf("[individual:1:n in {person}.senses]") should equal ("true\n")    
    
  @Test def testSenseInSensesFalse() = resultOf("[individual:1:n in {person:3:n}.senses]") should equal ("false\n")
    
  @Test def testSensePinSensesTrue() = resultOf("[{individual:1:n}.senses pin {person}.senses]") should equal ("true\n")
    
  @Test def testSensePinSensesFalse() = resultOf("[{individual:1:n}.senses pin {person:1:n}.senses]") should equal ("false\n")
    
  // comparison exceptions -- after exception refactoring  
  
  //
  // logical operators
  //  
    
  @Test def testAndTrue() = resultOf("[1 < 2 and 2 < 3]") should equal ("true\n")
    
  @Test def testAndFalse() = resultOf("[1 < 2 and 3 < 3]") should equal ("false\n")
    
  @Test def testOrTrue() = resultOf("[1 < 2 or 3 < 3]") should equal ("true\n")
    
  @Test def testOrFalse() = resultOf("[1 < 1 or 4 < 3]") should equal ("false\n")
    
  @Test def testNotTrue() = resultOf("[not 3 < 3]") should equal ("true\n")
    
  @Test def testNotFalse() = resultOf("[not 1 < 2]") should equal ("false\n")
    
  // test or and not () precedence    
  
  //
  // in path checks NOW
  //
      
  // [# < 2]
  @Test def testSingleBackReference() = resultOf("{person:1}.senses[# != individual:1:n]") should equal ("person:1:n\nsomeone:1:n\nsomebody:1:n\nmortal:1:n\nsoul:2:n\n")
    
  @Test def testRelationAfterSingleBackReference() = resultOf("{person:1}.senses[# != individual:1:n].word") should equal ("mortal\nperson\nsomebody\nsomeone\nsoul\n")
    
  @Test def testTwoSingleBackReferences() = resultOf("{person}.senses[# != person:2:n].word[# != mortal]") should equal ("individual\nperson\nsomebody\nsomeone\nsoul\n")    
    
  @Test def testSingleAndDoubleBackReference() = resultOf("{person}.senses.word[## != individual:1:n and # != mortal]") should equal ("person\nsomebody\nsomeone\nsoul\n")

  @Test def testSingleBackReferenceDotHypernyms() = resultOf("{person}[{organism:1:n} in #.hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReference() = resultOf("{person}[{organism:1:n} in hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
 
  @Test def testSingleBackReferenceDotHypernymsWords() = resultOf("{person}[organism in #.hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
  @Test def testHashFreeBackReferenceDotHypernymsWords() = resultOf("{person}[organism in hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def testSingleBackReferenceDotHypernymsCount() = resultOf("{person}[count(#.hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsCount() = resultOf("{person}[count(hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testHashFreeBackReferenceDotHypernymsOrHyponymsWords() = resultOf("{person}[organism in hypernym|^hypernym.words]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
    
}