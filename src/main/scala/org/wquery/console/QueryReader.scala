package org.wquery.console

import java.io.Reader
import java.io.BufferedReader

class QueryReader(reader: Reader) {
  private val in = new BufferedReader(reader)
  
  def close = in.close
  
  def readQuery = {
    val builder = new StringBuilder    
    var stop = false
    var eof = false
    
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
              in reset 
          }
        case '/' =>
          in.mark(1)          
          in.read() match {            
            case '*' =>
              eof = skipMultiLineComment()  
            case _ =>
              builder append '/'
              in reset 
          }
        case chr =>
          builder append chr.asInstanceOf[Char]  
      }
      
      if (eof) {
        stop = true
      }
    }
    
    val query = builder.toString
    if (eof && query.trim == "") null else query
  }
  
  private def skipSingleLineComment(): Boolean = {
    do {
      in.read match {
        case -1 => return true
        case '\n' => return false                   
        case _ =>
      }
    } while (true)
      
    return false
  }  
  
  private def skipMultiLineComment(): Boolean = {
    do {
      in.read match {
        case -1 => return true
        case '*' => 
          in.read match {
            case -1 =>  return true
            case '/' => return false
            case _ =>  
          }                        
        case _ =>
      }
    } while (true)
      
    return false
  }  
}
