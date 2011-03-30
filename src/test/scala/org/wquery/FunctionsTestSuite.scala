package org.wquery
import org.testng.annotations.Test

class FunctionsTestSuite extends WQueryTestSuite {
  // aggregates
   
  @Test def testCount() = result of ("count({})") should equal ("91\n")  

  @Test def testMin() = result of ("min(1..10)") should equal ("1\n")  
    
  @Test def testMinEmpty() = result of ("min(zzzzzz)") should equal ("(no result)\n")  
  
  @Test def testMax() = result of ("max(1..10)") should equal ("10\n")
  
  @Test def testMaxEmpty() = result of ("max(zzzzzz)") should equal ("(no result)\n")  

  @Test def testSumInt() = result of ("sum(1..10)") should equal ("55\n")      

  @Test def testSumFloat() = result of ("sum(1..10*1.0)") should equal ("55.0\n")    
    
  @Test def testAvgInt() = result of ("avg(1..10)") should equal ("5.5\n")
    
  @Test def testAvgFloat() = result of ("avg(1..10*1.0)") should equal ("5.5\n")
    
  @Test def testSize() = result of ("size({car:1}.hypernym!)") should equal ("2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n")                                                                                    
    
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

}
