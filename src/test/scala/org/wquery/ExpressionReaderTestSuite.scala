package org.wquery

import java.io.StringReader

import org.scalatest.Matchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import org.wquery.reader.{ExpressionReader, InputLineReader}

import scala.collection.mutable.ListBuffer

class ExpressionReaderTestSuite extends TestNGSuite with Matchers {

  @Test def simpleQuery() = result shouldNotChange
    """{person}
      |""".stripMargin

  @Test def noQuery() = result of "" should equal (Nil)

  @Test def emptyLine() = result of "\n" should equal (Nil)

  @Test def simpleQueryWithAComment() = result only
    """{person} -- here is a comment {car}
       |""".stripMargin should equal ("{person} \n")

  @Test def commentAtTheBeginning() = result of
    """-- {person}
       |""".stripMargin should equal (Nil)

  @Test def hashCommentAtTheBeginning() = result of
    """# {person}
       |""".stripMargin should equal (Nil)

  @Test def simpleDoEnd() = result shouldNotChange
    """from {}$a do
      |   emit $a
      |end
      |""".stripMargin

  @Test def nestedDoEnd() = result shouldNotChange
    """from {}$a do
      |  do
      |     emit $a
      |     emit $a
      |  end
      |end
      |""".stripMargin

  @Test def nestedTwoDoEnds() = result shouldNotChange
    """from {}$a do
      |  do
      |     emit $a
      |     emit $a
      |  end
      |  do
      |     emit $a
      |  end
      |end
      |""".stripMargin


  @Test def threeQueries() = result of
    """{person}
      |{person}
      |{person}
      |""".stripMargin should equal (List("{person}\n", "{person}\n", "{person}\n"))

  @Test def threeQueriesSeparatedBySpaces() = result of
    """{person}
      |
      |{person}
      |
      |{person}
      |""".stripMargin should equal (List("{person}\n", "{person}\n", "{person}\n"))

  @Test def fourComplexQueries() = result of
    """from {}$a do
       |  do
       |    emit $a
       |    emit $a
       |  end
       |end
       |{}
       |do
       |  do
       |    emit $a
       |    emit $a
       |  end
       |end
       |from {}$a do
       |  do
       |    emit $a
       |    emit $a
       |  end
       |end
       |""".stripMargin should equal (List(
    """from {}$a do
       |  do
       |    emit $a
       |    emit $a
       |  end
       |end
       |""".stripMargin,
    "{}\n",
    """do
      |  do
      |    emit $a
      |    emit $a
      |  end
      |end
      |""".stripMargin,
    """from {}$a do
      |  do
      |    emit $a
      |    emit $a
      |  end
      |end
      |""".stripMargin))

  @Test def fourComplexQueriesWithNewLines() = result of
    """from {}$a do
      |  do
      |    emit $a
      |    emit $a
      |  end
      |end
      |
      |{}
      |
      |do
      |  do
      |
      |    emit $a
      |    emit $a
      |  end
      |end
      |
      |from {}$a do
      |  do
      |    emit $a
      |
      |    emit $a
      |  end
      |end
      |""".stripMargin should equal (List(
    """from {}$a do
      |  do
      |    emit $a
      |    emit $a
      |  end
      |end
      |""".stripMargin,
    "{}\n",
    """do
      |  do
      |
      |    emit $a
      |    emit $a
      |  end
      |end
      |""".stripMargin,
    """from {}$a do
      |  do
      |    emit $a
      |
      |    emit $a
      |  end
      |end
      |""".stripMargin))


  //
  // initialization
  //

  object ResultOf {
    def of(query: String) = {
      val results = new ListBuffer[String]
      val reader = new ExpressionReader(new InputLineReader(new StringReader(query)))

      reader.foreach(results.append(_))

      reader.close()
      results.toList
    }

    def only(query: String) =  of(query).head

    def shouldNotChange(query: String) = only(query) should equal (query)
  }

  val result = ResultOf
}
