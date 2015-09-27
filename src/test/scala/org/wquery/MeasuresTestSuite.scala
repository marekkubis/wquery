package org.wquery

import org.testng.annotations.Test

class MeasuresTestSuite extends WQueryTestSuite {
  @Test def testMinSize() = result of "min_size({car}.^hypernym?)" should equal ("1\n")

  @Test def testMaxSize() = result of "max_size({car}.^hypernym?)" should equal ("2\n")

  @Test def testLCS() = result of "lcs({bus:4:n}, {taxi:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testLCSOnHypernymyPath() = result of "lcs({bus:4:n}, {car:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testLCSOnEqualSynsets() = result of "lcs({car:1:n}, {auto:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testMinPathLength() = result of "min_path_length({bus:4:n}, {taxi:1:n})" should equal ("3\n")

  @Test def testMinPathLengthOnHypernymyPath() = result of "min_path_length({bus:4:n}, {car:1:n})" should equal ("2\n")

  @Test def testMinPathLengthOnEqualSynsets() = result of "min_path_length({car:1:n}, {auto:1:n})" should equal ("1\n")

  @Test def testPathMeasure() = result of "path_measure({bus:4:n}, {taxi:1:n})" should startWith ("0.333")

  @Test def testPathMeasureOnHypernymyPath() = result of "path_measure({bus:4:n}, {car:1:n})" should equal ("0.5\n")

  @Test def testPathMeasureOnEqualSynsets() = result of "path_measure({car:1:n}, {auto:1:n})" should equal ("1.0\n")

  @Test def testHierarchyDepthForBusCar() = result of "hierarchy_depth({bus:4:n}, {car:1:n})" should equal ("13\n")

  @Test def testHierarchyDepthForBusTaxi() = result of "hierarchy_depth({bus:4:n}, {taxi:1:n})" should equal ("13\n")

  @Test def testLchMeasure() = result of "lch_measure({bus:4:n}, {taxi:1:n})" should equal ("2.159484249353372\n")

}
