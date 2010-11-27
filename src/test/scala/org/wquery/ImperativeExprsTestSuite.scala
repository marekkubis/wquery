package org.wquery

import org.testng.annotations.Test

class ImperativeExprsTestSuite extends WQueryTestSuite {
  @Test def testIterationAndEmission() = result of ("from $a in {person} emit $a") should equal ("{ person:3:n }\n{ person:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def testBlockAndAssignment() = result of ("from $a, $b in {person:1:n}, {person:2:n} do $c, $d = $b, $a emit $c, $d end") should equal ("{ person:2:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def testIf() = result of ("if [1 > 2] emit 3") should equal ("(no result)\n")        
    
  @Test def testIfElse() = result of ("if [1 < 2] emit 3 else emit 4") should equal ("3\n")    

  @Test def testInvertedRelationAlias() = {
    result of ("hyponyms=^hypernym") should equal ("(no result)\n")
    result of ("{car:1:n}.hyponyms") should equal ("{ ambulance:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ compact:3:n compact car:1:n }\n{ convertible:1:n }\n{ coupe:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ electric:1:n electric automobile:1:n electric car:1:n }\n{ gas guzzler:1:n }\n{ hardtop:1:n }\n{ hatchback:1:n }\n{ horseless carriage:1:n }\n{ hot rod:1:n hot-rod:1:n }\n{ jeep:1:n landrover:1:n }\n{ limousine:1:n limo:1:n }\n{ loaner:2:n }\n{ minicar:1:n }\n{ minivan:1:n }\n{ Model T:1:n }\n{ pace car:1:n }\n{ racer:2:n race car:1:n racing car:1:n }\n{ roadster:1:n runabout:1:n two-seater:1:n }\n{ sedan:1:n saloon:3:n }\n{ sports car:1:n sport car:1:n }\n{ sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ Stanley Steamer:1:n }\n{ stock car:1:n }\n{ subcompact:1:n subcompact car:1:n }\n{ touring car:1:n phaeton:1:n tourer:2:n }\n{ used-car:1:n secondhand car:1:n }\n")
  }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
  @Test def testInvertedAndClosedRelationAlias() = {
    result of ("transitive_hyponyms=^hypernym!") should equal ("(no result)\n")      
    result of ("{car:1:n}.transitive_hyponyms") should equal ("{ ambulance:1:n }\n{ beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ berlin:3:n }\n{ brougham:2:n }\n{ bus:4:n jalopy:1:n heap:3:n }\n{ cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ compact:3:n compact car:1:n }\n{ convertible:1:n }\n{ coupe:1:n }\n{ cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ electric:1:n electric automobile:1:n electric car:1:n }\n{ finisher:5:n }\n{ funny wagon:1:n }\n{ gas guzzler:1:n }\n{ gypsy cab:1:n }\n{ hardtop:1:n }\n{ hatchback:1:n }\n{ horseless carriage:1:n }\n{ hot rod:1:n hot-rod:1:n }\n{ jeep:1:n landrover:1:n }\n{ limousine:1:n limo:1:n }\n{ loaner:2:n }\n{ minicab:1:n }\n{ minicar:1:n }\n{ minivan:1:n }\n{ Model T:1:n }\n{ pace car:1:n }\n{ panda car:1:n }\n{ racer:2:n race car:1:n racing car:1:n }\n{ roadster:1:n runabout:1:n two-seater:1:n }\n{ sedan:1:n saloon:3:n }\n{ shooting brake:1:n }\n{ sports car:1:n sport car:1:n }\n{ sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ Stanley Steamer:1:n }\n{ stock car:2:n }\n{ stock car:1:n }\n{ subcompact:1:n subcompact car:1:n }\n{ touring car:1:n phaeton:1:n tourer:2:n }\n{ used-car:1:n secondhand car:1:n }\n")
  }
    
}
