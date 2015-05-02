package org.wquery
import org.testng.annotations.Test

class FunctionsTestSuite extends WQueryTestSuite {
  // aggregates
   
  @Test def testCount() = result of "count({})" should equal ("99\n")

  @Test def testMin() = result of "min(1..10)" should equal ("1\n")

  @Test def testMinMultipleResults() = result withoutDistinctAndSort "emit min(1..10 union 1..5)" should equal ("1\n1\n")

  @Test def testMinEmpty() = result of "min(zzzzzz)" should equal ("(no result)\n")  
  
  @Test def testMax() = result of "max(1..10)" should equal ("10\n")

  @Test def testMaxMultipleResults() = result withoutDistinctAndSort "emit max(1..10 union 6..10)" should equal ("10\n10\n")

  @Test def testMaxEmpty() = result of "max(zzzzzz)" should equal ("(no result)\n")  

  @Test def testShortest() = result of "shortest({car}.hypernym+)" should equal ("{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def testLongest() = result of "longest({car}.hypernym+)" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  @Test def testSumInt() = result of "sum(1..10)" should equal ("55\n")      

  @Test def testSumFloat() = result of "sum(1..10*1.0)" should equal ("55.0\n")    
    
  @Test def testAvgInt() = result of "avg(1..10)" should equal ("5.5\n")
    
  @Test def testAvgFloat() = result of "avg(1..10*1.0)" should equal ("5.5\n")
    
  @Test def testSize() = result of "size({car:1}.hypernym+)" should equal ("2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n")

  @Test def testEmptyFalse() = result of "empty(1..10)" should equal ("false\n")

  @Test def testEmptyTrue() = result of "empty(1 except 1)" should equal ("true\n")

  // scalars

  @Test def testAbsInt() = result of "abs(-1)" should equal ("1\n")    
    
  @Test def testAbsFloat() = result of "abs(-1.0)" should equal ("1.0\n")

  @Test def testCeil() = result of "ceil(12.1)" should equal ("13.0\n")
    
  @Test def testExp() = result of "exp(1)" should startWith ("2.71")

  @Test def testFloor() = result of "floor(12.1)" should equal ("12.0\n")
    
  @Test def testLogFloat() = result of "log(10.0)" should equal ("2.302585092994046\n")    

  @Test def testLogInt() = result of "log(10)" should equal ("2.302585092994046\n")    
    
  @Test def testPowerFloat() = result of "power(2.0, 3.0)" should equal ("8.0\n")

  @Test def testPowerInt() = result of "power(2, 3)" should equal ("8.0\n")

  @Test def testInt() = result of "int(2.0)" should equal ("2\n")

  @Test def testFloat() = result of "float(2)" should equal ("2.0\n")

  @Test def testRandom() = result of "random()" should fullyMatch regex "[01]\\.[0-9]+\n"

  @Test def testRound() = result of "round(1.6)" should equal ("2\n")

  @Test def testSqrt() = result of "sqrt(4.0)" should equal ("2.0\n")

  // string functions
   
  @Test def testStringLength() = result of "string_length(person)" should equal ("6\n")

  @Test def testStringSplit() = result of "string_split((`a s d f` union `x y z`),` `)" should equal ("a s d f\nx y z\n")

  @Test def testSubstringFromTo() = result of "substring(person, 2, 4)" should equal ("rs\n")

  @Test def testReplace() = result of "replace(person, `r..n`, `t`)" should equal ("pet\n")

  @Test def testLower() = result of "lower(`TEST`)" should equal ("test\n")

  @Test def testUpper() = result of "upper(`test`)" should equal ("TEST\n")

  @Test def testRange() = result of "range(1..10,7..15 union 23,25)" should equal ("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n23\n24\n25\n")

  @Test def testSort() = result of "emit sort(10..15 union 23..25 union 1..9)" should equal ("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n23\n24\n25\n")

  @Test def testSortBy() = result withoutDistinctAndSort "emit sortby((10..15,2 union 23..25,1 union 1..9,3),2)" should equal ("23 1\n24 1\n25 1\n10 2\n11 2\n12 2\n13 2\n14 2\n15 2\n1 3\n2 3\n3 3\n4 3\n5 3\n6 3\n7 3\n8 3\n9 3\n")

  @Test def testMinBy() = result of "minby((10..15,2 union 23..25,1 union 1..9,3),2)" should equal ("23 1\n24 1\n25 1\n")

  @Test def testMaxBy() = result of "maxby((10..15,2 union 23..25,1 union 1..9,3),2)" should equal ("1 3\n2 3\n3 3\n4 3\n5 3\n6 3\n7 3\n8 3\n9 3\n")

  @Test def testProd() = result of "prod(1..3,3)" should equal ("1 1 1\n1 1 2\n1 1 3\n1 2 3\n1 3 3\n2 3 3\n3 3 3\n")

  @Test def testDistinct() = result of "emit distinct(1..3 union 1..4)" should equal ("1\n2\n3\n4\n")

  @Test def testFlatten() = result of "flatten(1,2,3 union 4,5 union 6,7)" should equal ("1\n2\n3\n4\n5\n6\n7\n")

  @Test def testArcName() = result of "arc_name(\\hypernym)" should equal ("hypernym\n")

  @Test def testSourceName() = result of "arc_src(\\hypernym)" should equal ("src\n")

  @Test def testDestinationName() = result of "arc_dst(\\hypernym)" should equal ("dst\n")

  @Test def testAsTupleWords() = result of "as_tuple(`car\tz12345\tauto`)" should equal ("car auto\n")

  @Test def testAsTupleStrings() = result of "as_tuple(`car\t\\`z12345\\`\tauto`)" should equal ("car z12345 auto\n")

  @Test def testAsTupleSenses() = result of "as_tuple(`car:5:n\tcar:245:v\tauto:1:n`)" should equal ("car:5:n auto:1:n\n")

  @Test def testAsTupleSynsets() = result of "as_tuple(`{car:5:n}\t{car:245:v}\t{auto:1:n}`)" should equal ("{ cable car:1:n car:5:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testAsTupleNumbers() = result of "as_tuple(`1.3\t234\t-1\t-1.3`)" should equal ("1.3 234 -1 -1.3\n")

  @Test def testAsTupleVarious() = result of "as_tuple(`car:1:n\tcar\t-1\t1.3\t{person:3:n}`)" should equal ("car:1:n car -1 1.3 { person:3:n }\n")

  @Test def testAsTupleSeparator() = result of "as_tuple(`car:1:n#car#{person:3:n}#4#5`,`/#/`)" should equal ("car:1:n car { person:3:n } 4 5\n")

  @Test def testFormatString() = result of "format(`%s`, car)" should equal ("car\n")

  @Test def testFormatSense() = result of "format(`%s`, car:1:n)" should equal ("car:1:n\n")

  @Test def testFormatInt() = result of "format(`%d`, 3)" should equal ("3\n")

  @Test def testFormatFloat() = result of "format(`%2.3f`, 1/3)" should equal ("0.333\n")

  @Test def testFormatStringAsInt() = result of "format(`%d`, car)" should equal ("ERROR: Invalid format string: d != java.lang.String\n")

  @Test def testFormatIntAsString() = result of "format(`%s`, 3)" should equal ("3\n")

  @Test def testFormatFloatAsInt() = result of "format(`%2d`, 1/3)" should equal ("ERROR: Invalid format string: d != java.lang.Double\n")

  @Test def testFormatIntAsFloat() = result of "format(`%2.3f`, 3)" should equal ("ERROR: Invalid format string: f != java.lang.Integer\n")

  @Test def testFormatIncorectFormat() = result of "format(`%2.3d`, 3)" should equal ("ERROR: Invalid format string: 3\n")

  // is functions

  @Test def tesIsSynsetTrue() = result of "is_synset({car:1:n})" should equal ("true\n")

  @Test def tesIsSynsetFalse() = result of "is_synset(1)" should equal ("false\n")

  @Test def tesIsSenseTrue() = result of "is_sense(car:1:n)" should equal ("true\n")

  @Test def tesIsSenseFalse() = result of "is_sense(1)" should equal ("false\n")

  @Test def tesIsStringTrue() = result of "is_string(car)" should equal ("true\n")

  @Test def tesIsStringFalse() = result of "is_string(1)" should equal ("false\n")

  @Test def tesIsIntegerTrue() = result of "is_integer(1)" should equal ("true\n")

  @Test def tesIsIntegerFalse() = result of "is_integer(1.0)" should equal ("false\n")

  @Test def tesIsFloatTrue() = result of "is_float(1.0)" should equal ("true\n")

  @Test def tesIsFloatFalse() = result of "is_float(1)" should equal ("false\n")

  @Test def tesIsBooleanTrue() = result of "is_boolean(false)" should equal ("true\n")

  @Test def tesIsBooleanFalse() = result of "is_boolean(1)" should equal ("false\n")

  @Test def tesIsArcTrue() = result of "is_arc(\\hypernym)" should equal ("true\n")

  @Test def tesIsArcFalse() = result of "is_arc(1)" should equal ("false\n")

}
