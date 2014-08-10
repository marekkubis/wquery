package org.wquery.printer

import java.io.{OutputStream, OutputStreamWriter}

import org.wquery.model._

import scala.xml.{Utility, XML}

class LmfPrinter extends WordNetPrinter {
  override def print(wordNet: WordNet, output: OutputStream) = {
    val synsets = wordNet.fetch(WordNet.SynsetSet, List((Relation.Src, Nil)), List(Relation.Src))
    val words = wordNet.fetch(WordNet.WordSet, List((Relation.Src, Nil)), List(Relation.Src))


    val wordNetXml =
      <LexicalResource>
        <Lexicon>
          {for (List(wordForm: String) <- words.paths)
        yield {
          val senses = wordNet.getSenses(wordForm).groupBy(_.pos)

          for ((pos, posSenses) <- senses)
          yield {
            <LexicalEntry>
              <Lemma writtenForm={wordForm} partOfSpeech={pos}/>{for (sense <- posSenses)
            yield
                <Sense id={sense.toString} synset={wordNet.getSynset(sense).map(_.id).getOrElse("")}/>}
            </LexicalEntry>
          }
        }}{for (List(synset: Synset) <- synsets.paths)
        yield
          <Synset id={synset.id}>
            {wordNet.schema.getRelation("desc", Map(Relation.Src -> Set(SynsetType), Relation.Dst -> Set(StringType)))
            .map { relation =>
            val destinations = wordNet.fetch(relation, List((Relation.Src, List(synset))), List(Relation.Dst))

            destinations.paths.headOption match {
              case Some(List(gloss: String)) =>
                  <Definition gloss={gloss}/>
            }
          }.getOrElse()}<SynsetRelations>
            {for (relation <- wordNet.relations)
            yield {
              if (relation.sourceType == SynsetType && relation.destinationType.exists(_ == SynsetType)) {
                val destinations = wordNet.fetch(relation, List((Relation.Src, List(synset))), List(Relation.Dst))
                for (List(destinationSynset: Synset) <- destinations.paths) yield {
                    <SynsetRelation target={destinationSynset.id} relType={relation.name}/>
                }
              }
            }}
          </SynsetRelations>
          </Synset>}
        </Lexicon>
      </LexicalResource>
    val writer = new OutputStreamWriter(output)
    XML.write(writer, Utility.trim(wordNetXml), "utf-8", true, null)
    writer.flush()
  }
}
