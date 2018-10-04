package com.fca.conceptgeneration
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GraphConstruction extends Serializable {
  case class Concept(extent: Set[String], intent: Set[String], isValidNeighbor: Boolean, parent_index: Int)

  def main(args: Array[String]) {

    implicit val sc = new SparkContext("local", "graph-construction")
    val generatedConcepts =sc.textFile("/Users/raghavendrakumar/workspace/ConceptAnalysis/fcaop3/*/part-00000")
   val allConcepts= generatedConcepts.filter{ f => f.contains("true")
     
      
    }
    
    allConcepts. coalesce(1).distinct().saveAsTextFile("totalConcepts5")
   
    
  }

}