/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lsh4s

import java.io.{ File, FileOutputStream }
// import scala.pickling.Pickler
// import scala.pickling.Defaults._
// import scala.pickling.binary._
import breeze.linalg._
import scala.util.Random
import org.slf4s.Logging
import scala.io.Source

// how to make it picklible: https://gitter.im/scala/pickling?at=566b2c826a17cd3b36dc85d8
class LSH(vectors: Map[Long, Seq[Double]], hashGroups: Seq[Seq[Hash]]) {
  // def save(path: String) = {
  //   //implicit val p: Pickler[Map[String, Seq[Long]]] = Pickler.generate[Map[String, Seq[Long]]]
  //   val fo = new FileOutputStream(path)
  //   val so = new StreamOutput(fo)
  //   this.pickleTo(so)
  //   fo.close()
  // }
  
  def query(vector: Seq[Double], maxReturnSize: Int) = {
    val v = DenseVector(vector.toArray)
    hashGroups.flatMap { hashGroup =>
      hashGroup.zipWithIndex.flatMap { case (hash, i) =>
        val sectionSize = math.pow(0.5, i + 1.0)
        val hashStr = hash.randomVectors.map { r =>
          (((v dot DenseVector(r.toArray)) + hash.randomShift) / sectionSize).floor.toInt
        }.mkString(",")
        hash.buckets.get(hashStr).getOrElse(Seq.empty)
      }
    }.distinct.map { id =>
      val t = DenseVector(vectors(id).toArray)
      id -> norm(t - v)
    }.sortBy(_._2).map(_._1).take(maxReturnSize).toSeq
  }
  
  def query(id: Long, maxReturnSize: Int): Seq[Long] = {
    query(vectors(id), maxReturnSize)
  }
}

class Hash(
  val randomVectors: Seq[Seq[Double]],
  val randomShift: Double,
  val buckets: Map[String, Seq[Long]]
)

object LSH extends Logging {
  def hash(
    inputVectors: Map[Long, Seq[Double]],
    numOfHashGroups: Int,
    maxLevel: Int,
    bucketThreshold: Int
  ): LSH = {
    log.info("scaling vectors")
    val vectors = {
      val wrappedVectors = inputVectors.mapValues(v => DenseVector(v.toArray))
      val scale = 1.0 / wrappedVectors.values.map(v => norm(v)).max
      wrappedVectors.mapValues(_ * scale)
    }
  
    val dimension = inputVectors.values.head.size
    
    def levelHash(
      hashes: Seq[Hash],
      groupId: Int,
      level: Int,
      numOfRandomVectors: Int,
      vectors: Map[Long, DenseVector[Double]],
      sectionSize: Double
    ): Seq[Hash] = {
      val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
      val randomShift = Random.nextDouble * sectionSize
      
      log.info(s"hashing group $groupId level $level")
      val allBuckets = vectors.par.mapValues { v =>
        randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
      }.toSeq.groupBy(_._2).mapValues(_.map(_._1).seq)
      
      val smallBuckets = allBuckets.filter(_._2.size <= bucketThreshold || level == maxLevel).seq.toMap
      log.info(s"# of buckets: ${smallBuckets.size}, largest bucket: ${smallBuckets.values.map(_.size).max}")
      
      val hash = new Hash(randomVectors.map(_.data.toSeq), randomShift, smallBuckets)
      
      val remainVectors = {
        val largeBucketVectorIds = allBuckets.filter(_._2.size > bucketThreshold).values.flatten.toSet
        vectors.par.filterKeys(largeBucketVectorIds.contains).seq.toMap
      }
      if (remainVectors.nonEmpty) {
        levelHash(hashes :+ hash, groupId, level + 1, numOfRandomVectors + 2, remainVectors, sectionSize * 0.5)
      } else {
        hashes :+ hash
      }
    }
    
    val hashGroups = (1 to numOfHashGroups).map { groupId =>
      levelHash(Seq.empty, groupId, 0, 3, vectors, 0.5)
    }
    
    new LSH(vectors.mapValues(_.data.toSeq), hashGroups)
  }
  
  def hash(
    filename: String,
    numOfHashGroups: Int,
    maxLevel: Int,
    bucketThreshold: Int
  ): LSH = {
    val vectors = Source.fromFile(filename).getLines.map(_.split(" ")).map { arr =>
      val id = arr.head.toLong
      val vector = arr.tail.map(_.toDouble).toSeq
      id -> vector
    }.toMap
    hash(vectors, numOfHashGroups, maxLevel, bucketThreshold)
  }
  
}
