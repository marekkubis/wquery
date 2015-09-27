package org.wquery

import org.testng.annotations.Test

class MeasuresTestSuite extends WQueryTestSuite {
  @Test def testMinSize() = result of "min_size({car}.^hypernym?)" should equal ("1\n")

  @Test def testMaxSize() = result of "max_size({car}.^hypernym?)" should equal ("2\n")

  @Test def testLCS() = result of "lcs({bus:4:n}, {taxi:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testLCSOnHypernymyPath() = result of "lcs({bus:4:n}, {car:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testLCSOnEqualSynsets() = result of "lcs({car:1:n}, {auto:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
}
