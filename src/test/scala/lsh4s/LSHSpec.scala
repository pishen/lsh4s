package lsh4s

import org.scalatest._
import breeze.linalg._
import scala.io.Source
import scala.util.Random
import org.slf4s.Logging
import scalaj.http._

class LSHSpec extends FlatSpec with Logging {
  "LSH" should "have proper precisions" in {
    val vectors = Source.fromFile("input_vectors").getLines.map(_.split(" ")).map { arr =>
      val id = arr.head.toLong
      val vector = DenseVector(arr.tail.map(_.toDouble))
      id -> vector
    }.toMap
    
    val lsh = LSH.hash(vectors, 10, 10000, "mem")
    
    //val idMap = Source.fromFile("input_vectors").getLines.map(_.split(" ").head.toLong).zipWithIndex.toMap
    //val idRev = idMap.toSeq.sortBy(_._2).map(_._1)
    
    val precisions = Random.shuffle(vectors.toSeq).take(100).zipWithIndex.par.map { case ((id, vector), i) =>
      val answers = vectors
        .filterKeys(_ != id)
        .mapValues(t => norm(t - vector))
        .foldLeft(Map.empty[Long, Double]){ (b, a) =>
          val b2 = b + a
          if(b2.size <= 10) b2 else {
            val maxId = b2.maxBy(_._2)._1
            b2 - maxId
          }
        }
        .keySet
      
      val precision = lsh.query(id, 10).count(answers.contains) / answers.size.toDouble
      //val result = Http(s"http://localhost:10023/query/${idMap(id)}").asString.body.split(",").tail.map(_.toInt).map(idRev.apply)
      //log.info(s"answer: ${answers.toSeq.sorted.mkString(",")}\nresult: ${result.sorted.mkString(",")}")
      
      //val precision = result.count(answers.contains) / answers.size.toDouble
      
      log.info(s"vector: $id, precision: $precision")
      
      precision
    }
    
    val avgPrecision = precisions.sum / precisions.size.toDouble
    log.info(s"Average precision: $avgPrecision")
  }
}
