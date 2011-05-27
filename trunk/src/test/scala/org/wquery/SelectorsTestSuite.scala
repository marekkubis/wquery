package org.wquery
import org.testng.annotations.Test

class SelectorsTestSuite extends WQueryTestSuite {
  @Test def testPerson1nSynsetWithHypernyms() = result of ("{person:1:n}$a.hypernym$b<$a, $b>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { organism:1:n being:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")

  @Test def testPerson1nSynsetWithoutHypernyms() = result of ("{person:1:n}$a.hypernym<$a>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testPerson1nSynsetWithoutHypernymsAndWithoutTheirHypernyms() = result of ("{person:1:n}$a.hypernym.hypernym<$a>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testPerson1nSynsetWithoutHypernymsAndWithTheirHypernyms() = result of ("{person:1:n}$a.hypernym.hypernym$b<$a, $b>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { physical entity:1:n }\n")

  @Test def testPerson1nSynsetWithHypernymsAndWithoutTheirHypernyms() = result of ("{person:1:n}$a.hypernym$b.hypernym<$a,$b>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")

  @Test def testPerson1nSynsetWithHypernymsAndWithTheirHypernyms() = result of ("{person:1:n}$a.hypernym$b.hypernym$c<$a, $b, $c>") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n } { physical entity:1:n }\n")

  @Test def testPerson1nSynsetIdAndSenses() = result of ("{person:1:n}.id$a...senses$b<$a, $b>") should equal ("100007846 person:1:n\n100007846 individual:1:n\n100007846 someone:1:n\n100007846 somebody:1:n\n100007846 mortal:1:n\n100007846 soul:2:n\n")
      
}
