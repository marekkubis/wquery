package org.wquery.engine.operations

sealed abstract class Side

case object Left extends Side

case object Right extends Side