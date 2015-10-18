package org.wquery.similarity

import java.io._

import org.wquery.model.{Relation, WordNet}

import scala.collection.mutable.ListBuffer

class WTagger(wordNet: WordNet, maxSize: Option[Int], lowercaseLookup: Boolean, separator: String) {

  val maxCompoundSize = maxSize.getOrElse(findMaxCompoundSize)

  private def findMaxCompoundSize = {
    val dataSet = wordNet.fetch(WordNet.WordSet, List((Relation.Src, Nil)), List(Relation.Src))
    var maxCompoundSize = 0

    for (path <- dataSet.paths) {
      val size = path.head.asInstanceOf[String].filter(_ == ' ').length

      if (size > maxCompoundSize)
        maxCompoundSize = size
    }

    maxCompoundSize
  }

  def tagSentence(sentence: Seq[String]) = {
    val tags = sentence.map(_ => new ListBuffer[String]())
    var i = 0
    var j = Math.min(i + maxCompoundSize, sentence.length)

    while (i < sentence.length) {
      val word = sentence.slice(i, j).mkString(" ")
      val senses = if (lowercaseLookup)
        wordNet.getSenses(word) ++ wordNet.getSenses(word.toLowerCase)
      else
        wordNet.getSenses(word)

      if (senses.nonEmpty) {
        for (sense <- senses) {
          tags(i).append("B_" + sense)

          for (k <- (i + 1) until j) {
            tags(k).append("I_" + sense)
          }
        }

        i = j
      } else {
        if (j > i + 1) {
          j -= 1
        } else {
          i += 1
          j = Math.min(i + maxCompoundSize, sentence.length)
        }
      }
    }

    tags.map(_.toList)
  }

  def writeSentence(sentence: Seq[String], tags: Seq[List[String]], writer: Writer) {
    for (i <- sentence.indices) {
      writer.write(sentence(i))

      for (tag <- tags(i)) {
        writer.write(separator)
        writer.write(tag)
      }

      writer.write('\n')
    }

    writer.write('\n')
  }

  def tag(input: InputStream, output: OutputStream) {
    val writer = new BufferedWriter(new OutputStreamWriter(output))

    for (line <- scala.io.Source.fromInputStream(input).getLines()) {
      val sentence = line.split(' ')
      val tags = tagSentence(sentence)
      writeSentence(sentence, tags, writer)
    }

    writer.close()
  }
}
