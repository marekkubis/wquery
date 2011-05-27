package org.wquery
import java.io.StringReader
import org.testng.annotations.Test
import org.wquery.console.QueryReader

class QueryReaderTestSuite extends WQueryTestSuite {
  @Test def testSingleLineComment() = queryReaderFor("123-- comment").readQuery should equal ("123")
    
  @Test def testInLineComment() = queryReaderFor("123/*456*/7").readQuery should equal ("1237")
    
  @Test def testMultiLineComment() = queryReaderFor("123/*456\n789\n123\n*/99").readQuery should equal ("12399")

  @Test def testMultiLineCommentStart() = queryReaderFor("123/*456\n789\n123\n99").readQuery should equal ("123")    

  @Test def testSemiColon() = queryReaderFor("123; 12").readQuery should equal ("123")    
    
  private def queryReaderFor(query: String) = new QueryReader(new StringReader(query))
}
