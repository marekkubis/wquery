package org.wquery

import org.testng.annotations.Test

class MeasuresTestSuite extends WQueryTestSuite {
  @Test def testMinSize() = result of "min_size({car}.^hypernym?)" should equal ("1\n")
}
