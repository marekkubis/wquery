package org.wquery.engine

sealed abstract class Direction

case object Forward extends Direction
case object Backward extends Direction