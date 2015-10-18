package org.wquery

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.testng.annotations.Test
import org.wquery.similarity.WTagger

class WTaggerTestSuite extends WQueryTestSuite {

  @Test def tagOneSentence() = {
    val tagger = new WTagger(wupdate.wordNet, 5, "\t")
    val input = new ByteArrayInputStream("This is the car sentence with a cable car part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\nwith\na\ncable\tB_cable car:1:n\ncar\tI_cable car:1:n\npart\n.\n\n")
  }

  @Test def tagTwoSentences() = {
    val tagger = new WTagger(wupdate.wordNet, 5, "\t")
    val input = new ByteArrayInputStream("This is the car sentence .\nThis is a cable car sentence .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\n.\n\nThis\nis\na\ncable\tB_cable car:1:n\ncar\tI_cable car:1:n\nsentence\n.\n\n")
  }
}
