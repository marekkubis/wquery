package org.wquery
import org.testng.annotations.Test

class FunctionsTestSuite extends WQueryTestSuite {
  // aggregates
   
  @Test def testCount() = result of ("count({})") should equal ("90\n")

  @Test def testMin() = result of ("min(1..10)") should equal ("1\n")  
    
  @Test def testMinEmpty() = result of ("min(zzzzzz)") should equal ("(no result)\n")  
  
  @Test def testMax() = result of ("max(1..10)") should equal ("10\n")
  
  @Test def testMaxEmpty() = result of ("max(zzzzzz)") should equal ("(no result)\n")  

  @Test def testShortest() = result of ("shortest({car}.hypernym+)") should equal ("{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def testLongest() = result of ("longest({car}.hypernym+)") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  @Test def testSumInt() = result of ("sum(1..10)") should equal ("55\n")      

  @Test def testSumFloat() = result of ("sum(1..10*1.0)") should equal ("55.0\n")    
    
  @Test def testAvgInt() = result of ("avg(1..10)") should equal ("5.5\n")
    
  @Test def testAvgFloat() = result of ("avg(1..10*1.0)") should equal ("5.5\n")
    
  @Test def testSize() = result of ("size({car:1}.hypernym+)") should equal ("2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n")

  @Test def testEmptyFalse() = result of ("empty(1..10)") should equal ("false\n")

  @Test def testEmptyTrue() = result of ("empty(1 except 1)") should equal ("true\n")

  // scalars

  @Test def testAbsInt() = result of ("abs(-1)") should equal ("1\n")    
    
  @Test def testAbsFloat() = result of ("abs(-1.0)") should equal ("1.0\n")

  @Test def testCeil() = result of ("ceil(12.1)") should equal ("13.0\n")
    
  @Test def testFloor() = result of ("floor(12.1)") should equal ("12.0\n") 
    
  @Test def testLogFloat() = result of ("log(10.0)") should equal ("2.302585092994046\n")    

  @Test def testLogInt() = result of ("log(10)") should equal ("2.302585092994046\n")    
    
  @Test def testPowerFloat() = result of ("power(2.0, 3.0)") should equal ("8.0\n")

  @Test def testPowerInt() = result of ("power(2, 3)") should equal ("8.0\n")
    
  // string functions
   
  @Test def testLength() = result of ("string_length(person)") should equal ("6\n")

  @Test def testSubstringFrom() = result of ("substring(person, 2)") should equal ("rson\n")    
    
  @Test def testSubstringFromTo() = result of ("substring(person, 2, 4)") should equal ("rs\n")

  @Test def testReplace() = result of ("replace(person, `r..n`, `t`)") should equal ("pet\n")

  @Test def testLower() = result of ("lower(`TEST`)") should equal ("test\n")

  @Test def testUpper() = result of ("upper(`test`)") should equal ("TEST\n")

  @Test def testRange() = result of ("range(1..10,7..15 union 23,25)") should equal ("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n23\n24\n25\n")
}
