package org.wquery.model

class Sense(i:String, wf: String, sn: Int, ps: String) {
    val id = i
    val wordForm = wf
    val senseNumber = sn
    val pos = ps
    
    override def toString = wordForm + ":" + senseNumber + ":" + pos
}
