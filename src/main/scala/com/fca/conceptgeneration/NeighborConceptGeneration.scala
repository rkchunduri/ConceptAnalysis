package com.fca.conceptgeneration
import scala.collection.Map
import scala.collection.Set
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object NeighborConceptGeneration extends Serializable {
  case class Concept(extent: Set[String], intent: Set[String], isValidNeighbor: Boolean)
  var parent_index = 1
  def main(args: Array[String]) {
    println("Hello world")
    implicit val sc = new SparkContext("local", "concept-generation")

    // val inputFile = args(0)
    //implicit val sc = new SparkContext(new SparkConf().setAppName("concept-generation"));
    val inputFile = "/Users/raghavendrakumar/workspace/ConceptAnalysis/context2.txt"

    val context = sc.textFile(inputFile).map {
      line =>
        val data = line.split(",")

        (data.head, data.tail.mkString(","))
    }
    val contextInverse = sc.textFile(inputFile).map {
      line =>
        val data = line.split(",")
        val attributes = data.tail.mkString(",").split(",").map { f =>
          val attribute = f

          (attribute, data.head)
        }.toList

        (attributes)
    }

    val attributesinContext = sc.textFile(inputFile).map {
      line =>

        val data = line.split(",")
        val attributes = data.tail.mkString(",").split(",").map { f =>

          val attribute = f

          (attribute)
        }.toSet
        (attributes)
    }
    val allAttributes = attributesinContext.reduce { (a, b) => a.union(b) }

    val contextAsMap = context.collectAsMap()
    val contextInverseAsMap = contextInverse.map { f =>
      val contextInverseListMerge = f
      (contextInverseListMerge)
    }.reduce(_ union _).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toSet) }

    val commonObjectsamongAttributes = allAttributes.map(f => contextInverseAsMap.get(f)).flatten.reduce { (a, b) => a.intersect(b) }

    val leastFormalConcept = Concept(commonObjectsamongAttributes, allAttributes, true)

    val minSet = context.map { f =>
      val objectsInContext = f._1
      (objectsInContext)
    }.collect().toSet.diff(leastFormalConcept.extent)

    getFormalConceptforEachObject(sc, leastFormalConcept, contextInverseAsMap, contextAsMap, minSet, Set(leastFormalConcept), true, args(1))
    println("concept generation job done")

  }

  def getFormalConceptforEachObject(sc: SparkContext, concept: Concept, contextInverseAsMap: Map[String, Set[String]], contextAsMap: Map[String, String], minSet: Set[String], validUpperNeighborConcepts: Set[Concept], isLeastConcept: Boolean, opfile: String): Any = {
    val format = new SimpleDateFormat("ddHHmmssSSS")
    val validConcepts = if (concept.isValidNeighbor) {
    
    
     

      println("entered with concept details" + concept.extent + concept.intent + concept.isValidNeighbor)

      val differenceSet = sc.parallelize(contextAsMap.keySet.diff(concept.extent).toSeq)

      val validConcept = differenceSet.map { currentObject =>

        val commonObjectAsSet = Set(currentObject)
        val objectsRequiredforB1 = concept.extent.union(commonObjectAsSet)

        val forEachObjectInB1 = objectsRequiredforB1.map {
          p =>
            val attributesForEachObjectAsString = contextAsMap.apply(p).split(",").toSet

            (attributesForEachObjectAsString)
        }

        val B1 = forEachObjectInB1.reduce { (a, b) => (a.intersect(b)) }
        val listOfIntents = validUpperNeighborConcepts.map { concept =>
          val intentOfConcept = concept.intent
          (intentOfConcept)
        }.flatten

        val validNeighbors = if (!(B1.isEmpty) || isLeastConcept) {

          val B1asRDD = B1.map {
            f =>
              val rddElementasSet = Set(f)
              (rddElementasSet)
          }

          //if lenght is one consider the value from contextInverseAsMap
          val conceptsFound = if (B1.size == 1) {
            val a = B1asRDD.map { f =>
              val singleRDD = f.map { g =>
                val data = g
                (data)
              }.mkString
              (singleRDD)
            }

            (Concept(contextInverseAsMap.get(a.take(1).mkString).toSet.flatten, B1, true))
          } else {

            val conceptExtentEach = B1asRDD.map { f =>

              val extentData = contextInverseAsMap.apply(f.mkString)
              (extentData)
            }.reduce { (a, b) => a.intersect(b) }

            (Concept(conceptExtentEach, B1, true))
          }

          val differenceinComparisonSet = (conceptsFound.extent.diff(concept.extent)).diff(commonObjectAsSet)
          val validNeighborConcept = if (minSet.intersect(differenceinComparisonSet).size == 0) {

            (Concept(conceptsFound.extent, B1, true))

          } else {
            minSet.-(currentObject)

            Concept(conceptsFound.extent, B1, true)

          }
           
           
        
          (validNeighborConcept)
        } else {
          //parent_index =parent_index -1
          (Concept(Set(), Set(), false))

        }
        (validNeighbors)
      }
       val  validConcpetsForFile = validConcept.filter(f => f.isValidNeighbor ==true)
       if(validConcpetsForFile.count() !=0){
        parent_index = parent_index + 1
        //validConcept.foreach(println)
       // validConcpetsForFile.cache()
        validConcpetsForFile.distinct().saveAsTextFile(opfile + format.format(Calendar.getInstance().getTime()) + "_" + parent_index)
      
       }
      
       (validConcept)
    } else {
      return

    }

    val validConceptsInSet = validConcepts.collect().toSet

    validConceptsInSet.map {

      f =>

        getFormalConceptforEachObject(sc, f, contextInverseAsMap, contextAsMap, minSet, validUpperNeighborConcepts, false, opfile)

    }

  }
}


