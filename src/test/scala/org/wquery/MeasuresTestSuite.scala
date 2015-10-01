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

  @Test def testRootDistForCar() = result of "root_dist({car:1:n})" should equal ("11\n")

  @Test def testRootDistForBus() = result of "root_dist({'motor vehicle':1:n})" should equal ("10\n")

  @Test def testLcsDistForBus() = result of "lcs_dist({bus:4:n}, {'motor vehicle':1:n})" should equal ("2\n")

  @Test def testLcsDistForCar() = result of "lcs_dist({car:1:n}, {'motor vehicle':1:n})" should equal ("1\n")

  @Test def testLcsDistForMotorVehicle() = result of "lcs_dist({'motor vehicle':1:n}, {'motor vehicle':1:n})" should equal ("0\n")

  @Test def testWupMeasure() = result of "wup_measure({bus:4:n}, {taxi:1:n})" should startWith ("0.9166666666666666")

  @Test def testWupMeasureOnHypernymyPath() = result of "wup_measure({bus:4:n}, {car:1:n})" should startWith ("0.9565217391304348")

  @Test def testWupMeasureOnEqualSynsets() = result of "wup_measure({car:1:n}, {auto:1:n})" should equal ("1.0\n")

  @Test def testIC() = result of "ic({car:1:n})" should equal ("0.6931471805599453\n")

  @Test def testResnikMeasure() = result of "res_measure({bus:4:n}, {taxi:1:n})" should equal ("0.6931471805599453\n")

  @Test def testJiangConrathMeasure() = result of "jcn_measure({bus:4:n}, {taxi:1:n})" should equal ("0.2583177668073288\n")

  @Test def testLinMeasure() = result of "lin_measure({bus:4:n}, {taxi:1:n})" should equal ("0.2636796160574091\n")

}
