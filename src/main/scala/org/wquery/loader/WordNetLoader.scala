package org.wquery.loader

import org.wquery.model.WordNet

trait WordNetLoader {
  def canLoad(url:String): Boolean
  
  def load(url:String): WordNet   
}
