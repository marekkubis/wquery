package org.wquery
import org.testng.annotations.Test

class MultiPathsTestSuite extends WQueryTestSuite {  
  @Test def testUnion() = result of ("{person:1} union {person:2}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n }\n")

  @Test def testExcept() = result of ("{person} except {person:1}") should equal ("{ person:2:n }\n{ person:3:n }\n")    
    
  @Test def testIntersect() = result of ("{person} intersect {person:1}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
        
  @Test def testComma() = result of ("{person}, {car}") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { cable car:1:n car:5:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { car:4:n elevator car:1:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { car:3:n gondola:3:n }\n{ person:2:n } { cable car:1:n car:5:n }\n{ person:2:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ person:2:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ person:2:n } { car:4:n elevator car:1:n }\n{ person:2:n } { car:3:n gondola:3:n }\n{ person:3:n } { cable car:1:n car:5:n }\n{ person:3:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ person:3:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n{ person:3:n } { car:4:n elevator car:1:n }\n{ person:3:n } { car:3:n gondola:3:n }\n")
    
  // test precedense
  
}
