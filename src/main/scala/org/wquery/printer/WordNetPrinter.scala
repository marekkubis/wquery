package org.wquery.printer

import java.io.OutputStream

import org.wquery.model.WordNet

trait WordNetPrinter {
  def print(wordNet: WordNet, output: OutputStream)
}
