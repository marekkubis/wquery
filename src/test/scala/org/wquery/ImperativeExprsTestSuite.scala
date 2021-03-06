package org.wquery
import org.testng.annotations.Test

class ImperativeExprsTestSuite extends WQueryTestSuite {
  @Test def testStepIterationAndEmission() = result of "from {person}$a emit $a" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n{ person:3:n }\n")

  @Test def testPathIterationAndEmission() = result of "from {person}.hypernym@P emit @P" should equal ("hypernym { organism:1:n being:2:n }\nhypernym { causal agent:1:n cause:4:n causal agency:1:n }\nhypernym { human body:1:n physical body:1:n material body:1:n soma:3:n build:2:n figure:2:n physique:2:n anatomy:2:n shape:3:n bod:1:n chassis:1:n frame:3:n form:5:n flesh:2:n }\nhypernym { grammatical category:1:n syntactic category:1:n }\n")
  
  @Test def testBlockAndAssignment() = result of "from {person:1:n}$a, {person:2:n}$b do %c := $b %d := $a emit %c, %d end" should equal ("{ person:2:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testMultipleAssignments() = result of "do %c,%d := {car}.hypernym emit %c emit %d end" should equal ("hypernym { compartment:2:n }\nhypernym { motor vehicle:1:n automotive vehicle:1:n }\nhypernym { wheeled vehicle:1:n }\n{ cable car:1:n car:5:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ car:4:n elevator car:1:n }\n{ car:3:n gondola:3:n }\n")

  @Test def testIf() = result of "if [1 > 2] emit 3" should equal ("(no result)\n")        
    
  @Test def testIfElse() = result of "if [1 < 2] emit 3 else emit 4" should equal ("3\n")    

  @Test def testInvertedRelationAlias() = {
    result of "update aliases += `hypernym`, `dst`, `src`, `hyponyms`" should equal ("(no result)\n")
    result of "last({car:1:n}.hyponyms)" should equal ("{ ambulance:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ compact:3:n compact car:1:n }\n{ convertible:1:n }\n{ coupe:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ electric:1:n electric automobile:1:n electric car:1:n }\n{ gas guzzler:1:n }\n{ hardtop:1:n }\n{ hatchback:1:n }\n{ horseless carriage:1:n }\n{ hot rod:1:n hot-rod:1:n }\n{ jeep:1:n landrover:1:n }\n{ limousine:1:n limo:1:n }\n{ loaner:2:n }\n{ minicar:1:n }\n{ minivan:1:n }\n{ Model T:1:n }\n{ pace car:1:n }\n{ racer:2:n race car:1:n racing car:1:n }\n{ roadster:1:n runabout:1:n two-seater:1:n }\n{ sedan:1:n saloon:3:n }\n{ sports car:1:n sport car:1:n }\n{ sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ Stanley Steamer:1:n }\n{ stock car:1:n }\n{ subcompact:1:n subcompact car:1:n }\n{ touring car:1:n phaeton:1:n tourer:2:n }\n{ used-car:1:n secondhand car:1:n }\n")
  }

  @Test def testWhileSetVarDo() = {
    result of "%p := {car}" should equal ("(no result)\n")
    result of "while [ count(%p) < 50 ] do %p := %p union {car} emit count(%p) end" should equal ("10\n15\n20\n25\n30\n35\n40\n45\n50\n")
  }

  @Test def testFunctionIdentity() = {
    result of "function identity emit %A" should equal ("(no result)\n")
    result of "identity(1)" should equal ("1\n")
  }

  @Test def testFunctionDuplicateColumn() = {
    result of "function dup_col emit %A,%A" should equal ("(no result)\n")
    result of "dup_col(1)" should equal ("1 1\n")
  }

  @Test def testFunctionDuplicateRow() = {
    result of "function dup_row do emit %A emit %A end" should equal ("(no result)\n")
    result of "count(dup_row(1))" should equal ("2\n")
  }

  @Test def testFunctionDuplicateValue() = {
    result of "function dup_val emit %A + %A" should equal ("(no result)\n")
    result of "dup_val(2)" should equal ("4\n")
  }

  @Test def testFunctionGetHypernyms() = {
    result of "function get_hypernyms emit %A.hypernym" should equal ("(no result)\n")
    result of "get_hypernyms({car:1:n})" should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n")
  }
}
