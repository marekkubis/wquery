package org.wquery
import org.testng.annotations.Test

class ArithmeticsTestSuite extends WQueryTestSuite {
  @Test def testMinusOne() = result of ("-1") should equal ("-1\n")
    
  @Test def testMinusMinusOne() = result of ("-(-1)") should equal ("1\n")
    
  @Test def testIntPlusInt() = result of ("2 + 2") should equal ("4\n")
    
  @Test def testIntPlusDouble() = result of ("2 + 2.1") should equal ("4.1\n")
    
  @Test def testDoublePlusInt() = result of ("2.1 + 2") should equal ("4.1\n")
    
  @Test def testDoublePlusDouble() = result of ("2.1 + 2.1") should equal ("4.2\n")
    
  @Test def testIntMinusInt() = result of ("2 - 3") should equal ("-1\n")
    
  @Test def testIntMulInt() = result of ("2 * 3") should equal ("6\n")
    
  @Test def testIntDivInt() = result of ("7 / 3") should equal ("2\n")
    
  @Test def testIntModInt() = result of ("7 % 3") should equal ("1\n")    

  @Test def testIntPlusString() = result of ("2 + `a`") should startWith ("ERROR: Operator '+' requires paths that end with float or integer values")  

  @Test def testFloatMinusInt() = result of ("2.0 - 3") should equal ("-1.0\n")
    
  @Test def testFloatMulInt() = result of ("2.0 * 3") should equal ("6.0\n")
    
  @Test def testFloatDivInt() = result of ("7.0 / 3") should startWith ("2.33")
    
  @Test def testFloatModInt() = result of ("7.0 % 3") should equal ("1.0\n")    
}
