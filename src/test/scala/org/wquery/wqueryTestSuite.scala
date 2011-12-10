package org.wquery
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.BeforeClass
import org.wquery.emitter.{PlainWQueryEmitter, WQueryEmitter}

object WQueryRuntime {
  val wquery = createWQuery
  val emitter = new PlainWQueryEmitter

  def createWQuery = {
    val wquery = WQuery.createInstance("src/main/assembly/template/samplenet.xml")

    wquery.execute("\\desc^source required_by := true")
    wquery.execute("\\desc^source functional := true")
    wquery.execute("\\desc^source functional_action := `preserve`")
    wquery
  }
}

abstract class WQueryTestSuite extends TestNGSuite with ShouldMatchers {  
  var wquery: WQuery = null
  var emitter: WQueryEmitter = null        
  
  class Result {
    def of(query: String) = emitter.emit(wquery.execute(query))
  }
  
  val result = new Result
  
  @BeforeClass def setUp() {
    wquery = WQueryRuntime.wquery
    emitter = WQueryRuntime.emitter
  }   
}
