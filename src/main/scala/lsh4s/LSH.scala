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

class LSH(db: DB) {
  val dbVectors = db.hashMap[Long, DenseVector[Double]]("vectors")
  val hashGroups = db.hashSet[Hash]("hashInfo").asScala.toSeq.groupBy(_.group).mapValues(_.sortBy(_.level))
  val dbBuckets = db.hashMap[String, Seq[Long]]("buckets")
  
  def query(vector: DenseVector[Double], maxReturnSize: Int) = {
    val candidates = hashGroups.values.toSeq.flatMap { hashes =>
      hashes.flatMap { h =>
        val hashStr = h.randomVectors.map(r => (((vector dot r) + h.randomShift) / h.sectionSize).floor.toInt).mkString(",")
        dbBuckets.get(s"${h.group},${h.level}#$hashStr")
      }
    }.distinct
    
    candidates
      .map(id => id -> dbVectors.get(id))
      .sortBy { case (id, t) => norm(t - vector) }
      .take(maxReturnSize)
      .map(_._1)
  }
  
  def query(id: Long, maxReturnSize: Int): Seq[Long] = {
    query(dbVectors.get(id), maxReturnSize)
  }
}

case class Hash(group: Int, level: Int, randomVectors: Seq[DenseVector[Double]], randomShift: Double, sectionSize: Double)

object LSH extends Logging {
  def hash(
    vectors: Map[Long, DenseVector[Double]],
    numOfHashGroups: Int,
    maxLevel: Int,
    bucketSize: Int,
    outputPath: String
  ): LSH = {
    val db = {
      if (outputPath == "mem") DBMaker.memoryDB() else {
        val file = new File(outputPath)
        assert(!file.exists)
        DBMaker.fileDB(file)
      }
    }.transactionDisable().make()
    
    log.info("inserting vectors")
    val dbVectors = db.hashMap[Long, DenseVector[Double]]("vectors")
    vectors.foreach {
      case (id, v) => dbVectors.put(id, v)
    }
    db.commit()
    
    val dbHashInfo = db.hashSet[Hash]("hashInfo")
    val dbBuckets = db.hashMap[String, Seq[Long]]("buckets")
    val dimension = vectors.values.head.length
    
    def levelHash(
      group: Int,
      level: Int,
      numOfRandomVectors: Int,
      sectionSize: Double,
      vectors: Map[Long, DenseVector[Double]]
    ): Unit = {
      val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
      val randomShift = Random.nextDouble * sectionSize
      
      dbHashInfo.add(Hash(group, level, randomVectors, randomShift, sectionSize))
      
      log.info(s"hashing group $group level $level")
      val allBuckets = vectors.par.mapValues { v =>
        randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
      }.toSeq.groupBy(_._2).mapValues(_.map(_._1).seq).seq
      
      val smallBuckets = allBuckets.filter(_._2.size <= bucketSize || level == maxLevel)
      log.info(s"# of buckets: ${smallBuckets.size}, largest bucket: ${smallBuckets.values.map(_.size).max}")
      
      log.info("inserting result")
      smallBuckets.foreach {
        case (hashStr, ids) => dbBuckets.put(s"$group,$level#$hashStr", ids)
      }
      db.commit()
      
      val remainVectors = {
        val largeBucketVectorIds = allBuckets.filter(_._2.size > bucketSize).values.flatten.toSet
        vectors.par.filterKeys(largeBucketVectorIds.contains).seq.toMap
      }
      if(remainVectors.nonEmpty) levelHash(group, level + 1, numOfRandomVectors + 2, sectionSize * 0.5, remainVectors)
    }
    
    (1 to numOfHashGroups).foreach { groupId =>
      levelHash(groupId, 0, 3, 0.5, vectors)
    }
    
    if(outputPath == "mem"){
      new LSH(db)
    } else {
      db.close()
      new LSH(DBMaker.fileDB(new File(outputPath)).readOnly().make())
    }
  }

  def hash(
    filename: String,
    numOfHashGroups: Int,
    maxLevel: Int,
    bucketSize: Int,
    outputPath: String
  ): LSH = {
    val vectors = Source.fromFile(filename).getLines.map(_.split(" ")).map { arr =>
      val id = arr.head.toLong
      val vector = DenseVector(arr.tail.map(_.toDouble))
      id -> vector
    }.toMap
    hash(vectors, numOfHashGroups, maxLevel, bucketSize, outputPath)
  }
}
