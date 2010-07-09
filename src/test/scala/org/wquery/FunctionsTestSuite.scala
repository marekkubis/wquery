package org.wquery

import org.testng.annotations.Test

class FunctionsTestSuite extends WQueryTestSuite {
  // aggregates
   
  @Test def testCount() = resultOf("count({})") should equal ("91\n")  

  @Test def testMin() = resultOf("min(1..10)") should equal ("1\n")  
    
  @Test def testMax() = resultOf("max(1..10)") should equal ("10\n")  

  @Test def testSumInt() = resultOf("sum(1..10)") should equal ("55\n")      

  @Test def testSumFloat() = resultOf("sum(1..10*1.0)") should equal ("55.0\n")    
    
  @Test def testAvgInt() = resultOf("avg(1..10)") should equal ("5.5\n")
    
  @Test def testAvgFloat() = resultOf("avg(1..10*1.0)") should equal ("5.5\n")
    
  @Test def testSize() = resultOf("size({car:1}.<hypernym>!)") should equal ("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n")                                                                                    
    
  // scalars

  @Test def testAbsInt() = resultOf("abs(-1)") should equal ("1\n")    
    
  @Test def testAbsFloat() = resultOf("abs(-1.0)") should equal ("1.0\n")

  @Test def testCeil() = resultOf("ceil(12.1)") should equal ("13.0\n")
    
  @Test def testFloor() = resultOf("floor(12.1)") should equal ("12.0\n") 
    
  @Test def testLogFloat() = resultOf("log(10.0)") should equal ("2.302585092994046\n")    

  @Test def testLogInt() = resultOf("log(10)") should equal ("2.302585092994046\n")    
    
  @Test def testPowerFloat() = resultOf("power(2.0, 3.0)") should equal ("8.0\n")

  @Test def testPowerInt() = resultOf("power(2, 3)") should equal ("8.0\n")
    
  // string functions
   
  @Test def testLength() = resultOf("length(person)") should equal ("6\n")

  @Test def testSubstringFrom() = resultOf("substring(person, 2)") should equal ("rson\n")    
    
  @Test def testSubstringFromTo() = resultOf("substring(person, 2, 4)") should equal ("rs\n")

  @Test def testReplace() = resultOf("replace(person, `r..n`, `t`)") should equal ("pet\n")

  @Test def testLower() = resultOf("lower(`TEST`)") should equal ("test\n")

  @Test def testUpper() = resultOf("upper(`test`)") should equal ("TEST\n")

}
