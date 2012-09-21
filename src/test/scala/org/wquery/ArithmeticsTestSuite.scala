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
    
  @Test def testIntDivInt() = result of ("7 div 3") should equal ("2\n")

  @Test def testIntDivFloatInt() = result of ("7.1 div 2") should equal ("3.0\n")

  @Test def testIntDivIntFloat() = result of ("7 div 2.1") should equal ("3.0\n")

  @Test def testIntDivFloatFloat() = result of ("7.1 div 2.1") should equal ("3.0\n")

  @Test def testIntDivisionInt() = result of ("7 / 3") should startWith ("2.33")
    
  @Test def testIntModInt() = result of ("7 mod 3") should equal ("1\n")

  @Test def testIntPlusString() = result of ("2 + `a`") should startWith ("ERROR: Operator '+' requires paths that end with float or integer values")  

  @Test def testMinusString() = result of ("- `a`") should startWith ("ERROR: Operator '-' requires a path that ends with float or integer values")

  @Test def testFloatMinusInt() = result of ("2.0 - 3") should equal ("-1.0\n")
    
  @Test def testFloatMulInt() = result of ("2.0 * 3") should equal ("6.0\n")
    
  @Test def testFloatDivInt() = result of ("7.0 / 3") should startWith ("2.33")
    
  @Test def testFloatModInt() = result of ("7.0 mod 3") should equal ("1.0\n")

  @Test def testEmptyDataSetArithmOperation() = result of ("zzzz.senses.sensenum/2") should equal ("(no result)\n")
}
