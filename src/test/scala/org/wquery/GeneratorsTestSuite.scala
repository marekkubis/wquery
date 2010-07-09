package org.wquery

import org.testng.annotations.Test

class GeneratorsTestSuite extends WQueryTestSuite {
  @Test def testPersonSynsets() = resultOf("{person}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")        
  
  @Test def testHumanBodySynsets() = resultOf("{'human body'}") should equal ("{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n")        

  @Test def testPerson1nSynset() = resultOf("{person:1:n}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")            
    
  @Test def testPerson255nSynset() = resultOf("{person:255:n}") should equal ("(no result)\n")            
    
  @Test def testPerson1nSense() = resultOf("person:1:n") should equal ("person:1:n\n")        
    
  @Test def testPerson1Senses() = resultOf("person:1") should equal ("person:1:n\n")        
    
  @Test def testHumanBody1nSense() = resultOf("'human body':1:n") should equal ("human body:1:n\n")            
    
  @Test def testA1b2c3d41nSSense() = resultOf("a1b2c3d4:1:n") should equal ("(no result)\n")        
    
  @Test def testPerson255nSense() = resultOf("person:255:n") should equal ("(no result)\n")        
    
  @Test def testPersonWord() = resultOf("person") should equal ("person\n")        
    
  @Test def testA1b2c3d4Word() = resultOf("a1b2c3d4") should equal ("(no result)\n")        
    
  @Test def testHumanBodyWord() = resultOf("'human body'") should equal ("human body\n")             

  @Test def testA1b2c3d4QuotedWord() = resultOf("'a1b2c3d4'") should equal ("(no result)\n")        

  @Test def testPersonBackQuotedWord() = resultOf("`person`") should equal ("person\n")        
    
  @Test def testA1b2c3d4BackQuotedWord() = resultOf("`a1b2c3d4`") should equal ("a1b2c3d4\n")        
    
  @Test def test123Integer() = resultOf("123") should equal ("123\n")        

  @Test def test12E3Double() = resultOf("12e3") should equal ("12000.0\n")        

  @Test def test12Dot3Double() = resultOf("12.3") should equal ("12.3\n")        
   
  @Test def test12Dot3E3Double() = resultOf("12.3e3") should equal ("12300.0\n")        
         
  @Test def testDot5Double() = resultOf(".5") should equal ("0.5\n")        
    
  @Test def test0Dot5E2Double() = resultOf("0.5e2") should equal ("50.0\n")        

  @Test def test0Dot5EPlus2Double() = resultOf("0.5e+2") should equal ("50.0\n")        

  @Test def test0Dot5EMinus2Double() = resultOf("0.5e-2") should equal ("0.005\n")                
    
  @Test def test3To8Sequence() = resultOf("3..8") should equal ("3\n4\n5\n6\n7\n8\n")

  @Test def testFilter() = resultOf("[1 = 1]") should equal ("true\n")    
    
  @Test def testParensPair() = resultOf("(person.senses).word") should equal ("person\n")

  @Test def testTwoParensPairs() = resultOf("((person).senses).word") should equal ("person\n")
    
  @Test def testRsonEnding() = resultOf("\"rson$\"") should equal ("person\n")
   
  @Test def testAllWords() = resultOf("count('')") should equal ("170\n")

  @Test def testAllSenses() = resultOf("count(::)") should equal ("187\n")    
      
  @Test def testAllSynsets() = resultOf("count({})") should equal ("91\n")    
  
}
