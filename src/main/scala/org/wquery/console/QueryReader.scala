package org.wquery.console
import java.io.{BufferedReader, Reader}

class QueryReader(reader: Reader) {
  private val in = new BufferedReader(reader)
  private var eof = false

  def close { in.close }

  def isEof = eof
  
  def readQuery = {
    val builder = new StringBuilder    
    var stop = false
    
    while (!stop) {       
      in.read match {
        case -1 => 
          eof = true
        case ';' => 
          stop = true          
        case '-' =>
          in.mark(1)
          in.read() match {
            case '-' =>
              eof = skipSingleLineComment()
            case _ =>
              builder append '-'
              in.reset
          }
        case '/' =>
          in.mark(1)          
          in.read() match {            
            case '*' =>
              eof = skipMultiLineComment()  
            case _ =>
              builder append '/'
              in.reset
          }
        case chr =>
          builder append chr.asInstanceOf[Char]  
      }
      
      if (eof)
        stop = true
    }
    
    val query = builder.toString

    if (query.trim == "")
      None
    else
      Some(query)
  }
  
  private def skipSingleLineComment(): Boolean = {
    do {
      in.read match {
        case -1 => true
        case '\n' => false
        case _ =>
      }
    } while (true)
      
    false
  }  
  
  private def skipMultiLineComment(): Boolean = {
    do {
      in.read match {
        case -1 => true
        case '*' => 
          in.read match {
            case -1 =>  true
            case '/' => false
            case _ =>  
          }                        
        case _ =>
      }
    } while (true)
      
    false
  }  
}
