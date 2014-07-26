package org.wquery.reader

trait LineReader {
    def readFirstLine: String

    def readNextLine: String
}
