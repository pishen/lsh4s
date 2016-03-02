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

import java.io.File
import scala.pickling.Pickler
import scala.pickling.Defaults._
import scala.pickling.binary._
import scala.pickling.io.TextFileOutput
import breeze.linalg._
import scala.util.Random

// how to make it picklible: https://gitter.im/scala/pickling?at=566b2c826a17cd3b36dc85d8
case class LSH(hashes: Seq[Hash]) {
  def save(path: String) = {
    implicit val p: Pickler[Map[String, Seq[Long]]] = Pickler.generate[Map[String, Seq[Long]]]

    val fileOut = new TextFileOutput(new File(path))
    this.pickleTo(fileOut)
    fileOut.close()
  }
}

case class Hash(
  randomVectors: Seq[Seq[Double]],
  sectionSize: Double,
  randomShift: Double,
  buckets: Map[String, Seq[Long]]
)

object LSH {
  def hash(
    vectors: Map[Long, Seq[Double]],
    numOfRandomVectors: Int,
    numOfLevels: Int
  ): LSH = {
    val dimension = vectors.values.head.size
    require(numOfRandomVectors > 0 && numOfLevels > 0 && dimension > 0)
    
    val scaledVectors = {
      val wrappedVectors = vectors.mapValues(v => DenseVector(v.toArray))
      val scale = 1.0 / wrappedVectors.values.map(v => norm(v)).max
      wrappedVectors.mapValues(_ * scale)
    }
    
    def levelHash(hashes: Seq[Hash], vectors: Map[Long, DenseVector[Double]]): Seq[Hash] = {
      val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
      val sectionSize = if(hashes.isEmpty) 0.5 else hashes.last.sectionSize * 0.5
      val randomShift = Random.nextDouble * sectionSize
      val allBuckets = vectors.mapValues { v =>
        randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
      }.toSeq.groupBy(_._2).mapValues(_.map(_._1))
      val smallBuckets = allBuckets.filter(_._2.size <= 10000)
      val largeBucketVectors = allBuckets.filter(_._2.size > 10000).values.flatten.toSet
      val newHashes = hashes :+ Hash(randomVectors.map(_.data.toSeq), sectionSize, randomShift, smallBuckets)
      val remainVectors = vectors.filterKeys(largeBucketVectors.contains)
      if(remainVectors.isEmpty) newHashes else levelHash(newHashes, remainVectors)
    }
    
    levelHash(Seq.empty, scaledVectors)
    ???
  }
  
  def hash(vectors: Map[Long, Seq[Double]]): LSH = hash(vectors, vectors.values.head.size, 10)
  
  def hash(filename: String): LSH = {
    ???
  }
}
