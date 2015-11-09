package org.wquery

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.testng.annotations.Test
import org.wquery.similarity.WTagger

class WTaggerTestSuite extends WQueryTestSuite {

  @Test def tagOneSentence() = {
    val tagger = new WTagger(wupdate.wordNet, None, None, Map(), false, "\t")
    val input = new ByteArrayInputStream("This is the car sentence with a cable car part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\nwith\na\ncable\tB_cable car:1:n\ncar\tI_cable car:1:n\npart\n.\n\n")
  }

  @Test def tagOneSentenceWithAtMostOneSense() = {
    val tagger = new WTagger(wupdate.wordNet, None, Some(1), Map(), false, "\t")
    val input = new ByteArrayInputStream("This is the car sentence with a cable car part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\nsentence\nwith\na\ncable\tB_cable car:1:n\ncar\tI_cable car:1:n\npart\n.\n\n")
  }

  @Test def tagTwoSentences() = {
    val tagger = new WTagger(wupdate.wordNet, None, None, Map(), false, "\t")
    val input = new ByteArrayInputStream("This is the car sentence .\nThis is a cable car sentence .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\n.\n\nThis\nis\na\ncable\tB_cable car:1:n\ncar\tI_cable car:1:n\nsentence\n.\n\n")
  }

  @Test def tagOneSentenceWithLowercaseLookup() = {
    val tagger = new WTagger(wupdate.wordNet, None, None, Map(), true, "\t")
    val input = new ByteArrayInputStream("This is the Car sentence with a CABLE CAR part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\nCar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\nwith\na\nCABLE\tB_cable car:1:n\nCAR\tI_cable car:1:n\npart\n.\n\n")
  }

  @Test def tagOneSentenceWithSingleWordExpressions() = {
    val tagger = new WTagger(wupdate.wordNet, Some(1), None, Map(), false, "\t")
    val input = new ByteArrayInputStream("This is the car sentence with a cable car part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\nwith\na\ncable\ncar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\npart\n.\n\n")
  }

  @Test def tagOneSentenceWithSurfaceForms() = {
    val tagger = new WTagger(wupdate.wordNet, Some(1), None, Map(("Xcar","car"), ("carY", "car")), false, "\t")
    val input = new ByteArrayInputStream("This is the Xcar sentence with a cable carY part .".getBytes)
    val output = new ByteArrayOutputStream()

    tagger.tag(input, output)
    output.toString should equal ("This\nis\nthe\nXcar\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\nsentence\nwith\na\ncable\ncarY\tB_car:1:n\tB_car:2:n\tB_car:3:n\tB_car:4:n\tB_car:5:n\npart\n.\n\n")
  }
}
