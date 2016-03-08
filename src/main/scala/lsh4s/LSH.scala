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

import breeze.linalg._
import org.slf4s.Logging
import scala.util.Random
import scala.io.Source
import org.mapdb._
import java.io.File
import scala.collection.JavaConverters._

class LSH(
  vectors: collection.Map[Long, DenseVector[Double]],
  hashInfo: Seq[Hash],
  buckets: collection.Map[String, Seq[Long]]
) {
  def query(vector: DenseVector[Double], maxReturnSize: Int) = {
    hashInfo
      .flatMap { h =>
        val hashStr = h.randomVectors.map(r => (((vector dot r) + h.randomShift) / h.sectionSize).floor.toInt).mkString(",")
        buckets.get(s"${h.group},${h.level}#$hashStr").getOrElse(Seq.empty)
      }
      .distinct
      .map(id => id -> norm(vectors(id) - vector))
      .foldLeft(Map.empty[Long, Double]){ (b, a) =>
        val b2 = b + a
        if(b2.size <= maxReturnSize) b2 else {
          val maxId = b2.maxBy(_._2)._1
          b2 - maxId
        }
      }
      .toSeq
      .sortBy(_._2)
      .map(_._1)
  }
  
  def query(id: Long, maxReturnSize: Int): Seq[Long] = {
    query(vectors(id), maxReturnSize + 1).filterNot(_ == id) //don't return the query itself
  }
}

case class Hash(group: Int, level: Int, randomVectors: Seq[DenseVector[Double]], randomShift: Double, sectionSize: Double)

object LSH extends Logging {
  def hash(
    vectors: Map[Long, DenseVector[Double]],
    numOfHashGroups: Int,
    bucketSize: Int,
    outputPath: String
  ): LSH = {
    val memoryMode = outputPath == "mem"
    
    val dimension = vectors.values.head.length
    
    def levelHash(
      currentHashes: Seq[Hash],
      currentBuckets: Map[String, Seq[Long]],
      group: Int,
      level: Int,
      numOfRandomVectors: Int,
      sectionSize: Double,
      vectors: Map[Long, DenseVector[Double]]
    ): (Seq[Hash], Map[String, Seq[Long]]) = {
      val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
      val randomShift = Random.nextDouble * sectionSize
      
      val allBuckets = vectors.mapValues { v =>
        val hashStr = randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
        s"$group,$level#$hashStr"
      }.toSeq.groupBy(_._2).mapValues(_.map(_._1))
      
      val smallBuckets = allBuckets.filter(_._2.size <= bucketSize || level == 10)
      log.info(s"group $group level $level: # of buckets: ${smallBuckets.size}, largest bucket: ${smallBuckets.values.map(_.size).max}")
      
      val remainVectors = {
        val largeBucketVectorIds = allBuckets.filter(_._2.size > bucketSize).values.flatten.toSet
        vectors.filterKeys(largeBucketVectorIds.contains)
      }
      if (remainVectors.nonEmpty) {
        levelHash(
          currentHashes :+ Hash(group, level, randomVectors, randomShift, sectionSize),
          currentBuckets ++ smallBuckets,
          group,
          level + 1,
          numOfRandomVectors + 2,
          sectionSize * 0.5,
          remainVectors
        )
      } else {
        (
          currentHashes :+ Hash(group, level, randomVectors, randomShift, sectionSize),
          currentBuckets ++ smallBuckets
        )
      }
    }
    
    val initialSectionSize = vectors.values.map(v => norm(v)).max * 0.5
    
    val (hashInfo, buckets) = (0 until numOfHashGroups).par.map { groupId =>
      levelHash(Seq.empty, Map.empty, groupId, 0, 3, initialSectionSize, vectors)
    }.reduce {
      (a, b) => (a._1 ++ b._1, a._2 ++ b._2)
    }
    
    if (memoryMode) {
      new LSH(vectors, hashInfo, buckets)
    } else {
      val db = {
        val file = new File(outputPath)
        assert(!file.exists, "DB file already exist.")
        DBMaker.fileDB(file)
          .fileMmapEnableIfSupported()
          .fileMmapCleanerHackEnable()
          .closeOnJvmShutdown()
          .make()
      }
      
      log.info("inserting vectors")
      val dbVectors = db.hashMap[Long, DenseVector[Double]]("vectors")
      vectors.toSeq.foreach {
        case (id, v) => dbVectors.put(id, v)
      }
      db.commit()
      
      val dbHashInfo = db.hashSet[Hash]("hashInfo")
      hashInfo.foreach(h => dbHashInfo.add(h))
      db.commit()
      
      log.info("inserting buckets")
      val dbBuckets = db.hashMap[String, Seq[Long]]("buckets")
      buckets.foreach {
        case (h, ids) => dbBuckets.put(h, ids)
      }
      db.commit()
      
      new LSH(
        dbVectors.asScala,
        dbHashInfo.asScala.toSeq,
        dbBuckets.asScala
      )
    }
  }

  def hash(
    filename: String,
    numOfHashGroups: Int,
    bucketSize: Int,
    outputPath: String
  ): LSH = {
    val vectors = Source.fromFile(filename).getLines.map(_.split(" ")).map { arr =>
      val id = arr.head.toLong
      val vector = DenseVector(arr.tail.map(_.toDouble))
      id -> vector
    }.toMap
    hash(vectors, numOfHashGroups, bucketSize, outputPath)
  }
  
  def readLSH(inputPath: String) = {
    val db = {
      val file = new File(inputPath)
      assert(!file.exists)
      DBMaker.fileDB(file)
        .fileMmapEnableIfSupported()
        .fileMmapCleanerHackEnable()
        .closeOnJvmShutdown()
        .make()
    }
    new LSH(
      db.hashMap[Long, DenseVector[Double]]("vectors").asScala,
      db.hashSet[Hash]("hashInfo").asScala.toSeq,
      db.hashMap[String, Seq[Long]]("buckets").asScala
    )
  }
}
