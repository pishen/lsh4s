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
import scalikejdbc._
import scala.util.Random
import java.io.File
import scala.io.Source

class LSH(inputPath: String) {
  val name = new File(inputPath).getName
  if(!ConnectionPool.isInitialized(name)){
    ConnectionPool.add(name, s"jdbc:h2:$inputPath", "sa", "")
  }
  
  val levelGroups = NamedDB(name).readOnly { implicit session =>
    sql"SELECT * FROM LEVEL_INFO".map { r =>
      (
        r.int("GROUP"),
        r.int("LEVEL"),
        r.string("RANDOM_VECTORS").split("|").map(_.split(",").map(_.toDouble)).map(DenseVector.apply).toSeq,
        r.double("RANDOM_SHIFT")
      )
    }.list.apply().toSeq.groupBy(_._1).mapValues(_.sortBy(_._2).map(t => t._3 -> t._4)).toSeq
  }
  
  def query(vector: Seq[Double], maxReturnSize: Int) = {
    val v = DenseVector(vector.toArray)
    NamedDB(name).readOnly { implicit session =>
      val candidates = levelGroups.flatMap { case (groupId, levelGroup) =>
        levelGroup.zipWithIndex.flatMap { case ((randomVectors, randomShift), i) =>
          val sectionSize = math.pow(0.5, i + 1.0)
          val hashStr = randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
          sql"SELECT VECTOR_IDS FROM BUCKETS WHERE HASH=${hashStr} AND GROUP=${groupId} AND LEVEL=${i}"
            .map(r => r.string("VECTOR_IDS").split(",").map(_.toLong).toSeq)
            .single.apply().getOrElse(Seq.empty)
        }
      }.distinct
      
      sql"SELECT * FROM VECTORS WHERE ID IN ${candidates}"
        .map(r => r.long("ID") -> DenseVector(r.string("VECTOR").split(",").map(_.toDouble))).list.apply()
        .sortBy { case (id, t) => norm(t - v) }
        .map(_._1)
        .take(maxReturnSize)
        .toSeq
    }
  }
  
  def query(id: Long, maxReturnSize: Int): Seq[Long] = {
    NamedDB(name).readOnly { implicit session =>
      val vector = sql"SELECT VECTOR FROM VECTORS WHERE ID=${id}"
        .map(r => r.string("VECTOR").split(",").map(_.toDouble).toSeq).single.apply.get
      query(vector, maxReturnSize)
    }
  }
}

object LSH {
  Class.forName("org.h2.Driver")
  
  def hash(
    vectors: Map[Long, DenseVector[Double]],
    outputPath: String,
    numOfHashGroups: Int = 5,
    maxLevel: Int = 10
  ): LSH = {
    val name = new File(outputPath).getName
    ConnectionPool.add(name, s"jdbc:h2:$outputPath", "sa", "")
  
    NamedDB(name).autoCommit { implicit session =>
      sql"""CREATE TABLE VECTORS (
              ID BIGINT PRIMARY KEY,
              VECTOR VARCHAR
            )
         """.execute.apply()
      sql"""CREATE TABLE LEVEL_INFO (
              GROUP INT,
              LEVEL INT,
              RANDOM_VECTORS VARCHAR,
              RANDOM_SHIFT DOUBLE
            )
         """.execute.apply()
      sql"""CREATE TABLE BUCKETS (
              GROUP INT,
              LEVEL INT,
              HASH VARCHAR,
              VECTOR_IDS VARCHAR
            )
         """.execute.apply()
      sql"CREATE INDEX HASH_IDX ON BUCKETS(HASH)".execute.apply()
    }
    
    NamedDB(name).autoCommit { implicit session =>
      val rows = vectors.toSeq.map { case (id, vector) => Seq.apply[Any](id, vector.data.mkString(",")) }
      sql"INSERT INTO VECTORS VALUES (?, ?)".batch(rows: _*).apply()
    }
  
    val dimension = vectors.values.head.size
    val numOfRandomVectors = dimension
    
    val scaledVectors = {
      val wrappedVectors = vectors.mapValues(v => DenseVector(v.toArray))
      val scale = 1.0 / wrappedVectors.values.map(v => norm(v)).max
      wrappedVectors.mapValues(_ * scale)
    }
    
    def levelHash(groupId: Int, level: Int, vectors: Map[Long, DenseVector[Double]], sectionSize: Double): Unit = {
      val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
      val randomShift = Random.nextDouble * sectionSize
      
      NamedDB(name).autoCommit { implicit session =>
        val randomVectorsStr = randomVectors.map(_.data.mkString(",")).mkString("|")
        sql"INSERT INTO LEVEL_INFO VALUES (${groupId}, ${level}, ${randomVectorsStr}, ${randomShift})".update.apply()
      }
      
      val allBuckets = vectors.mapValues { v =>
        randomVectors.map(r => (((v dot r) + randomShift) / sectionSize).floor.toInt).mkString(",")
      }.toSeq.groupBy(_._2).mapValues(_.map(_._1))
      
      val smallBuckets = allBuckets.filter(_._2.size <= 5000 || level == maxLevel)
      
      NamedDB(name).autoCommit { implicit session =>
        val rows = smallBuckets.toSeq.map {
          case (h, vectorIds) => Seq.apply[Any](groupId, level, h, vectorIds.mkString(","))
        }
        sql"INSERT INTO BUCKETS VALUES (?, ?, ?, ?)".batch(rows: _*).apply()
      }
      
      val remainVectors = {
        val largeBucketVectorIds = allBuckets.filter(_._2.size > 5000).values.flatten.toSet
        vectors.filterKeys(largeBucketVectorIds.contains)
      }
      if(remainVectors.nonEmpty) levelHash(groupId, level + 1, remainVectors, sectionSize * 0.5)
    }
    
    (1 to numOfHashGroups).foreach { groupId =>
      levelHash(groupId, 0, vectors, 0.5)
    }
    
    ConnectionPool.close(name)
    
    new LSH(outputPath)
  }
  
  def hash(vectors: Map[Long, Seq[Double]], outputPath: String): LSH = hash(vectors.mapValues(s => DenseVector(s.toArray)), outputPath)
  
  def hash(filename: String, outputPath: String): LSH = {
    val vectors = Source.fromFile(filename).getLines.map(_.split(" ")).map { arr =>
      val id = arr.head.toLong
      val vector = arr.tail.map(_.toDouble).toSeq
      id -> vector
    }.toMap
    hash(vectors, outputPath)
  }
  
  def readLSH(inputPath: String) = new LSH(inputPath)
}
