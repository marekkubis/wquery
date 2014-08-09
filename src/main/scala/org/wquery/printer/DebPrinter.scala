package org.wquery.printer

import java.io.{OutputStream, OutputStreamWriter}

import org.wquery.model.{Relation, Synset, SynsetType, WordNet}

import scala.xml._

class DebPrinter extends WordNetPrinter {
  override def print(wordNet: WordNet, output: OutputStream) = {
    val synsets = wordNet.fetch(WordNet.SynsetSet, List((Relation.Src, Nil)), List(Relation.Src))

    val wordNetXml =
      <SYNSETS>
        {for (List(synset: Synset) <- synsets.paths)
      yield
        <SYNSET>
          {for (relation <- wordNet.relations)
          yield {
            if (relation.sourceType == SynsetType && relation.destinationType.isDefined
              && relation.name != "words" && relation.name != "senses") {
              val destinations = wordNet.fetch(relation, List((Relation.Src, List(synset))), List(Relation.Dst))

              relation.destinationType.get match {
                case SynsetType =>
                  for (List(destinationSynset: Synset) <- destinations.paths) yield {
                    <ILR type={relation.name}>{destinationSynset.id}</ILR>
                  }
                case _ =>
                  for (List(destination) <- destinations.paths) yield {
                    <r>{destination}</r>.copy(label = relation.name.toUpperCase)
                  }
              }
            }
          }}
          <SYNONYM>
            {for (sense <- wordNet.getSenses(synset))
          yield <LITERAL sense={sense.senseNumber.toString}>{sense.wordForm}</LITERAL>}
          </SYNONYM>
        </SYNSET>}
      </SYNSETS>
    val writer = new OutputStreamWriter(output)
    XML.write(writer, wordNetXml, "utf-8", true, null)
    writer.flush()
  }
}
