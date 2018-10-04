package com.fca.conceptgeneration

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import com.github.karlhigley.spark.neighbors.ANN

import org.apache.spark.SparkConf
object LSHInput {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "LSHEmbeddings");

 //val inputRDD = sc.textFile("/Users/raghavendrakumar/Downloads/node2vec-master/node2vecop")
 //   val inputRDD = sc.textFile("/Users/raghavendrakumar/Downloads/OpenNE-master/5")
    val inputRDD = sc.textFile("/Users/raghavendrakumar/Downloads/deepwalk-master/example_graphs/op11")
     //val inputRDD = sc.textFile("/Users/raghavendrakumar/Downloads/deepwalk-master/context2op")
    val header = inputRDD.first()
    val embeedings_data = inputRDD.filter(row => row != header)
    inputRDD.collect().foreach(println)
    val points = getEmbeddingsinLSHFormat(embeedings_data)
    val conceptNodes = points.map(f => f._1).collect()
    val coneptVectors = points.map(f => f._2).collect()

    for (i <- 0 until coneptVectors.length) {

      for (j <- i+1 until coneptVectors.length) {

        println("Cosine Similarity between" + conceptNodes(i) + "and" + conceptNodes(j) + "is = " + CosineSimilarity.cosineSimilarity(coneptVectors(i), coneptVectors(j)))

      }
      
      println("=======================")
   

    }

    //     val ann =
    //      new ANN(64, "cosine")
    //        .setTables(1)
    //        .setSignatureLength(16)
    //
    //    val model1 = ann.train(points)   
    //
    //    
    //   val nn=model1.neighbors(5).map{case(user,neighborsList) =>(user,neighborsList.map(_._2).reduce((acc, elem) => (acc + elem)),neighborsList.toList)}
    //  
    //   nn.saveAsTextFile("LSHOPforFCA7") 

  }

  def getEmbeddingsinLSHFormat(input: RDD[String]): RDD[(Long, Array[Double])] = {
    input.map {
      line =>
        val fields = line.split(" ")
        val tail = fields.tail.map(x => x.toDouble)
        val sparseVectorEmbeddings = Vectors.dense(tail).toSparse.toArray
        // println(sparseVectorEmbeddings)
        (fields.head.toLong, sparseVectorEmbeddings)
    }

  }

}



