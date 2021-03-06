package com.fca.conceptgeneration
import scala.collection.Map
import scala.collection.Set

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ExampleSpark extends Serializable {
  case class Concept(extent: Set[String], intent: Set[String], isValidNeighbor: Boolean)
  var validNeighborConcpets: Set[Concept] = null

  def main(args: Array[String]) {
    println("Hello world")

    @transient implicit val sc = new SparkContext("local", "concept-generation")

    //val inputFile =args(0)
    //implicit val sc = new SparkContext(new SparkConf().setAppName("concept-generation"));
    val inputFile = "/Users/raghavendrakumar/workspace/ConceptAnalysis/adult_fca.txt"

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

    println("attributesinCOntext")
    allAttributes.foreach(println)

    val contextAsMap = context.collectAsMap()
    val contextInverseAsMap = contextInverse.map { f =>
      val contextInverseListMerge = f
      (contextInverseListMerge)
    }.reduce(_ union _).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toSet) }
    println("printing context inverse")
    contextInverseAsMap.foreach(println)

    val commonObjectsamongAttributes = allAttributes.map(f => contextInverseAsMap.get(f)).flatten.reduce { (a, b) => a.intersect(b) }

    println(commonObjectsamongAttributes)
    //context.filter { f => f._2.replace(",", "").length() == numberOfAttributes } //need to change this logic to number of values in a key==number of attributes
    val leastFormalConcept = Concept(commonObjectsamongAttributes, allAttributes, true)

    val minSet = sc.broadcast(context.map { f =>
      val objectsInContext = f._1
      (objectsInContext)
    }.collect().toSet.diff(leastFormalConcept.extent)).value

    validNeighborConcpets = Set(leastFormalConcept)

    //getAllFormalConcepts(Set(leastConceptAsConcept),contextInverseAsMap, contextAsMap, minSet)

    getFormalConceptforEachObject(sc, leastFormalConcept, contextInverseAsMap, contextAsMap, minSet, Set(leastFormalConcept), true)
    println("all  formal concepts")
    validNeighborConcpets.foreach(println)
    //  sc.parallelize(validNeighborConcpets.toSeq).saveAsTextFile(args(1))
    //hdfs+"/user/hadoop/LLROP"

  }
  def getFormalConceptforEachObject(sc: SparkContext, concept: Concept, contextInverseAsMap: Map[String, Set[String]], contextAsMap: Map[String, String], minSet: Set[String], validUpperNeighborConcepts: Set[Concept], isLeastConcept: Boolean): Any = {

    val validConcepts = if (concept.isValidNeighbor) {
      println("entered with concept details" + concept.extent + concept.intent + concept.isValidNeighbor)

      val differenceSet = sc.parallelize(contextAsMap.keySet.diff(concept.extent).toSeq)
      val validConcept = differenceSet.map { currentObject =>
        println("forEachCurrentObject")
        val commonObjectAsSet = Set(currentObject)
        val objectsRequiredforB1 = concept.extent.union(commonObjectAsSet)
        println("attributes for Each Object")
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

          println("I am valid now")
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

            println("Will come")
            (Concept(contextInverseAsMap.get(a.take(1).mkString).toSet.flatten, B1, true))
          } else {
            B1asRDD.foreach(println)
            val conceptExtentEach = B1asRDD.map { f =>

              val extentData = contextInverseAsMap.apply(f.mkString)
              (extentData)
            }.reduce { (a, b) => a.intersect(b) }
            println("Most of the cobcepts vist heree")

            (Concept(conceptExtentEach, B1, true))
          }

          val differenceinComparisonSet = (conceptsFound.extent.diff(concept.extent)).diff(commonObjectAsSet)
          val validNeighborConcept = if (minSet.intersect(differenceinComparisonSet).size == 0) {
            println("Will be in set with true")
            validNeighborConcpets = validNeighborConcpets.+(Concept(conceptsFound.extent, B1, true))
            (Concept(conceptsFound.extent, B1, true))

          } else {
            minSet.-(currentObject)
            println("will in be set but no upper concepts")
            validNeighborConcpets = validNeighborConcpets.+(Concept(conceptsFound.extent, B1, true))
            Concept(conceptsFound.extent, B1, false)

          }

          (validNeighborConcept)
        } else {
          (Concept(Set(), Set(), false))
        }
        (validNeighbors)
      }

      // println("UpperNeighborConcepts")
      // println(validConcept)
      (validConcept)

    } else {
      return
      // (Set(Concept(Set(), Set(), false)))
    }

    val validConceptsInSet = validConcepts.collect().toSet
    validConcepts.foreach {
      f =>  getFormalConceptforEachObject(sc, f, contextInverseAsMap, contextAsMap, minSet, validUpperNeighborConcepts, false)

    }

    //getConcepts(validConcepts, sc, contextInverseAsMap, contextAsMap, minSet, validUpperNeighborConcepts)

    (validUpperNeighborConcepts)

  }



  

}



