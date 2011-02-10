package org.wquery.model

class Sense(val id: String, val wordForm: String, val senseNumber: Int, val pos: String) {    
    override def toString = wordForm + ":" + senseNumber + ":" + pos
}
