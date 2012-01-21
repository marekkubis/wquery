package org.wquery

import engine.{Error, Answer}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.BeforeClass
import org.wquery.emitter.{PlainWQueryEmitter, WQueryEmitter}

object WQueryRuntime {
  val wquery = createWQuery
  val emitter = new PlainWQueryEmitter

  def createWQuery = {
    val wquery = WQuery.createInstance("src/main/assembly/template/samplenet.xml")

    wquery.execute("\\hypernym transitivity := true")
    wquery.execute("\\partial_meronym transitivity := true")
    wquery.execute("\\member_meronym transitivity := true")
    wquery.execute("\\hypernym transitivity_action := `restore`")
    wquery.execute("\\partial_meronym transitivity_action := `restore`")
    wquery.execute("\\member_meronym transitivity_action := `restore`")
    wquery.execute("\\hypernym symmetry := `antisymmetric`")
    wquery.execute("\\hypernym symmetry_action := `preserve`")
    wquery.execute("\\similar symmetry := `symmetric`")
    wquery.execute("\\similar symmetry_action := `restore`")
    wquery.execute("\\desc^source required_by := true")
    wquery.execute("\\desc^source functional := true")
    wquery.execute("\\desc^source functional_action := `preserve`")

    wquery
  }
}

abstract class WQueryTestSuite extends TestNGSuite with ShouldMatchers {  
  var wquery: WQuery = null
  var emitter: WQueryEmitter = null

  class TuplesOf {
    def of(query: String) = wquery.execute(query) match {
      case Answer(_, ans) =>
        ans
      case Error(err) =>
        throw new IllegalArgumentException("Query returned error instead of dataset: " + err)
    }
  }

  val tuples = new TuplesOf

  class ResultOf {
    def of(query: String) = emitter.emit(wquery.execute(query))
  }

  val result = new ResultOf

  @BeforeClass def setUp() {
    wquery = WQueryRuntime.wquery
    emitter = WQueryRuntime.emitter
  }   
}
