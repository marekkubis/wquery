package org.wquery
import org.testng.annotations.Test

class ProjectionsTestSuite extends WQueryTestSuite {
  @Test def testPerson1nSynsetWithHypernyms() = result of "{person:1:n}$a.hypernym$b<$a, $b>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { organism:1:n being:2:n }\n{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")

  @Test def testPerson1nSynsetWithoutHypernyms() = result of "{person:1:n}$a.hypernym<$a>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testPerson1nSynsetWithoutHypernymsAndWithoutTheirHypernyms() = result of "{person:1:n}$a.hypernym.hypernym<$a>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")

  @Test def testPerson1nSynsetWithoutHypernymsAndWithTheirHypernyms() = result of "{person:1:n}$a.hypernym.hypernym$b<$a, $b>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { physical entity:1:n }\n")

  @Test def testPerson1nSynsetWithHypernymsAndWithoutTheirHypernyms() = result of "{person:1:n}$a.hypernym$b.hypernym<$a,$b>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n }\n")

  @Test def testPerson1nSynsetWithHypernymsAndWithTheirHypernyms() = result of "{person:1:n}$a.hypernym$b.hypernym$c<$a, $b, $c>" should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { causal agent:1:n cause:4:n causal agency:1:n } { physical entity:1:n }\n")

  @Test def testPerson1nSynsetIdAndSenses() = result of "{person:1:n}$a<last($a.id), last($a.senses)>" should equal ("100007846 individual:1:n\n100007846 mortal:1:n\n100007846 person:1:n\n100007846 somebody:1:n\n100007846 someone:1:n\n100007846 soul:2:n\n")

  @Test def testTransformationAfterUnaryProjection() = result of "{car}$a<$a>.hypernym" should equal ("{ cable car:1:n car:5:n } hypernym { compartment:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def testTransformationAfterBinaryProjection() = result of "{car}$a.hypernym$b<$a,$b>.hypernym" should equal ("{ cable car:1:n car:5:n } { compartment:2:n } hypernym { room:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { wheeled vehicle:1:n } hypernym { container:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { wheeled vehicle:1:n } hypernym { vehicle:1:n }\n{ car:4:n elevator car:1:n } { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } { compartment:2:n } hypernym { room:1:n }\n")
}
