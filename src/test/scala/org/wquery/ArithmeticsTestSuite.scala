package org.wquery

import org.testng.annotations.Test

class ArithmeticsTestSuite extends WQueryTestSuite {
  @Test def testMinusOne() = resultOf("-1") should equal ("-1\n")
    
  @Test def testMinusMinusOne() = resultOf("-(-1)") should equal ("1\n")
    
  @Test def testIntPlusInt() = resultOf("2 + 2") should equal ("4\n")
    
  @Test def testIntPlusDouble() = resultOf("2 + 2.1") should equal ("4.1\n")
    
  @Test def testDoublePlusInt() = resultOf("2.1 + 2") should equal ("4.1\n")
    
  @Test def testDoublePlusDouble() = resultOf("2.1 + 2.1") should equal ("4.2\n")
    
  @Test def testIntMinusInt() = resultOf("2 - 3") should equal ("-1\n")
    
  @Test def testIntMulInt() = resultOf("2 * 3") should equal ("6\n")
    
  @Test def testIntDivInt() = resultOf("7 / 3") should equal ("2\n")
    
  @Test def testIntModInt() = resultOf("7 % 3") should equal ("1\n")    
    
}
