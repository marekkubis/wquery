package org.wquery.printer

import java.io.{OutputStream, OutputStreamWriter}

import org.wquery.model._

import scala.xml._

class GraphMLPrinter extends WordNetPrinter {
  def printNode(obj: Any) = obj match {
    case synset: Synset =>
      synset.id
    case true =>
      "TRUE"
    case false =>
      "FALSE"
    case other =>
      other.toString
  }

  override def print(wordNet: WordNet, output: OutputStream) = {
    val wordNetXml =
      <graphml xmlns="http://graphml.graphdrawing.org/xmlns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
        <key id="relation" for="edge" attr.name="relation" attr.type="string"/>
        <key id="name" for="node" attr.name="name" attr.type="string"/>
        <key id="type" for="node" attr.name="type" attr.type="string"/>
        <graph id="wordnet" edgedefault="directed">
          {for (List(synset: Synset) <- wordNet.fetch(WordNet.SynsetSet, List((Relation.Src, Nil)), List(Relation.Src)).paths)
        yield {
          <node id={printNode(synset)}>
            <data key="name">{synset.id}</data>
            <data key="type">synset</data>
          </node>
        }}
          {for (List(sense: Sense) <- wordNet.fetch(WordNet.SenseSet, List((Relation.Src, Nil)), List(Relation.Src)).paths)
        yield {
          <node id={printNode(sense)}>
            <data key="name">{printNode(sense)}</data>
            <data key="type">sense</data>
          </node>
        }}
          {for (List(word: String) <- wordNet.fetch(WordNet.WordSet, List((Relation.Src, Nil)), List(Relation.Src)).paths)
        yield {
          <node id={printNode(word)}>
            <data key="name">{printNode(word)}</data>
            <data key="type">word</data>
          </node>
        }}
          {for (List(pos: String) <- wordNet.fetch(WordNet.PosSet, List((Relation.Src, Nil)), List(Relation.Src)).paths)
        yield {
          <node id={printNode(pos)}>
            <data key="name">{printNode(pos)}</data>
            <data key="type">pos</data>
          </node>
        }}
          <node id="TRUE">
            <data key="name">TRUE</data>
            <data key="type">boolean</data>
          </node>
          <node id="FALSE">
            <data key="name">FALSE</data>
            <data key="type">boolean</data>
          </node>
          {for (relation <- wordNet.relations if relation.destinationType.isDefined && !WordNet.derivedRelations.contains(relation) && relation.name != "id")
        yield {
            val dataSet = wordNet.fetch(relation, List((Relation.Src, Nil)), List(Relation.Src, Relation.Dst))
            for (List(source, destination) <- dataSet.paths) yield {
              <edge source={printNode(source)} target={printNode(destination)}>
                <data key="relation">
                {relation.name}
                </data>
              </edge>
            }
        }}
        </graph>
      </graphml>

    val writer = new OutputStreamWriter(output)
    XML.write(writer, Utility.trim(wordNetXml), "utf-8", true, null)
    writer.flush()
  }
}
