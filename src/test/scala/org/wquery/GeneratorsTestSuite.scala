package org.wquery
import org.testng.annotations.Test

class GeneratorsTestSuite extends WQueryTestSuite {
  @Test def testPersonSynsets() = result of ("{person}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")        
  
  @Test def testHumanBodySynsets() = result of ("{'human body'}") should equal ("{ human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\n")        

  @Test def testPerson1nSynset() = result of ("{person:1:n}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")            
    
  @Test def testPerson255nSynset() = result of ("{person:255:n}") should equal ("(no result)\n")            
    
  @Test def testPerson1nSense() = result of ("person:1:n") should equal ("person:1:n\n")        
    
  @Test def testPerson1Senses() = result of ("person:1") should equal ("person:1:n\n")        
    
  @Test def testHumanBody1nSense() = result of ("'human body':1:n") should equal ("human body:1:n\n")            
    
  @Test def testA1b2c3d41nSSense() = result of ("a1b2c3d4:1:n") should equal ("(no result)\n")        
    
  @Test def testPerson255nSense() = result of ("person:255:n") should equal ("(no result)\n")        
    
  @Test def testPersonWord() = result of ("person") should equal ("person\n")        
    
  @Test def testA1b2c3d4Word() = result of ("a1b2c3d4") should equal ("(no result)\n")        
    
  @Test def testHumanBodyWord() = result of ("'human body'") should equal ("human body\n")             

  @Test def testA1b2c3d4QuotedWord() = result of ("'a1b2c3d4'") should equal ("(no result)\n")        

  @Test def testPersonBackQuotedWord() = result of ("`person`") should equal ("person\n")        
    
  @Test def testA1b2c3d4BackQuotedWord() = result of ("`a1b2c3d4`") should equal ("a1b2c3d4\n")        
    
  @Test def test123Integer() = result of ("123") should equal ("123\n")        

  @Test def test12E3Double() = result of ("12e3") should equal ("12000.0\n")        

  @Test def test12Dot3Double() = result of ("12.3") should equal ("12.3\n")        
   
  @Test def test12Dot3E3Double() = result of ("12.3e3") should equal ("12300.0\n")        
         
  @Test def testDot5Double() = result of (".5") should equal ("0.5\n")        
    
  @Test def test0Dot5E2Double() = result of ("0.5e2") should equal ("50.0\n")        

  @Test def test0Dot5EPlus2Double() = result of ("0.5e+2") should equal ("50.0\n")        

  @Test def test0Dot5EMinus2Double() = result of ("0.5e-2") should equal ("0.005\n")                
    
  @Test def test3To8Sequence() = result of ("3..8") should equal ("3\n4\n5\n6\n7\n8\n")

  @Test def testFilter() = result of ("[1 = 1]") should equal ("true\n")    
    
  @Test def testParensPair() = result of ("(person.senses).word") should equal ("person\n")

  @Test def testTwoParensPairs() = result of ("((person).senses).word") should equal ("person\n")
    
  @Test def testRsonEnding() = result of ("\"rson$\"") should equal ("person\n")
   
  @Test def testAllWords() = result of ("count('')") should equal ("170\n")

  @Test def testAllSenses() = result of ("count(::)") should equal ("187\n")    
      
  @Test def testAllSynsets() = result of ("count({})") should equal ("91\n")    
  
}
