package org.wquery

import org.testng.annotations.Test

class SelectorsTestSuite extends WQueryTestSuite {
  @Test def testPerson1nSynsetHypernyms() = resultOf("{person:1:n}.hypernym") should equal ("{ organism:1:n being:2:n }\n{ causal agent:1:n cause:4:n causal agency:1:n }\n")  
  
  @Test def testPerson1nSynsetWithHypernyms() = resultOf("<{person:1:n}>.<hypernym>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { organism:1:n being:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")    

  @Test def testPerson1nSynsetHypernymsIds() = resultOf("{person:1:n}.hypernym.id") should equal ("100004475\n100007347\n")
                                                                                                                                                                                                                              
  @Test def testPerson1nSynsetWithoutHypernyms() = resultOf("<{person:1:n}>.hypernym") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")                                                                                                                                                                                                                                  
                                                                                                                                                                                                                              
  @Test def testPerson1nSynsetHypernymsHypernyms() = resultOf("{person:1:n}.hypernym.hypernym") should equal ("{ physical entity:1:n }\n")  

  @Test def testPerson1nSynsetHypernymsHypernymsIds() = resultOf("{person:1:n}.hypernym.hypernym.id") should equal ("100001930\n")    
    
  @Test def testPerson1nSynsetWithoutHypernymsAndWithoutTheirHypernyms() = resultOf("<{person:1:n}>.hypernym.hypernym") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
     
  @Test def testPerson1nSynsetWithoutHypernymsAndWithTheirHypernyms() = resultOf("<{person:1:n}>.hypernym.<hypernym>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { physical entity:1:n }\n")                                                                                                   

  @Test def testPerson1nSynsetWithHypernymsAndWithoutTheirHypernyms() = resultOf("<{person:1:n}>.<hypernym>.hypernym") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")                                                                                                   

  @Test def testPerson1nSynsetWithHypernymsAndWithTheirHypernyms() = resultOf("<{person:1:n}>.<hypernym>.<hypernym>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n } { physical entity:1:n }\n")                                                                                                   
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
  @Test def testPerson1nSynsetIdAndSenses() = resultOf("{person:1:n}.<id>..<senses>") should equal ("100007846 person:1:n\n100007846 individual:1:n\n100007846 someone:1:n\n100007846 somebody:1:n\n100007846 mortal:1:n\n100007846 soul:2:n\n")    
      
}
