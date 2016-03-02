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

case class LSHWriter(
  inputPath: String,
  outputPath: String,
  initNumOfRandomVectors: Int = 4,
  initSectionSize: Double = 5.0,
  maxLevel: Int = 10,
  numOfLsh: Int = 10
) {
  def withInitNumOfRandomVectors(number: Int) = this.copy(initNumOfRandomVectors = number)
  def withInitSectionSize(size: Double) = this.copy(initSectionSize = size)
  def withMaxLevel(level: Int) = this.copy(maxLevel = level)
  def withNumOfLsh(num: Int) = this.copy(numOfLsh = num)

  def write(): Future[Unit] = Future {
    val modelFile = File(inputPath)
    assert(modelFile.exists, "model file doesn't exist.")

    val db = DBMaker.fileDB(File(outputPath).toJava)
      .transactionDisable()
      .make()
    
    db.atomicInteger("lsh_num").set(numOfLsh)
    db.commit()

    def getVectorArrays() = modelFile.newInputStream.gzipped.lines.map(_.split(" ")).map {
      arr => arr.head.toLong -> arr.tail.map(_.toDouble)
    }

    Logger.debug(s"computing scale for $inputPath")
    val scale = 1.0 / getVectorArrays().map { case (id, arr) => norm(DenseVector(arr)) }.max

    Logger.debug(s"saving vectors for $inputPath")
    val dbVectors = db.hashMap[Long, Array[Double]]("vectors")
    getVectorArrays().foreach { case (id, arr) => dbVectors.put(id, arr.map(_ * scale)) }
    db.commit()

    val dbRandomVectors = db.hashMap[String, Array[Array[Double]]]("random_vectors")
    val dbSecionSizes = db.hashMap[String, Double]("section_sizes")

    val dimension = getVectorArrays().next._2.size

    def computeHashs(index: Int) = {
      def computeHash(level: Int, numOfRandomVectors: Int, sectionSize: Double): Int = {
        val hashName = s"lsh${index}_level$level"
        Logger.debug(s"hashing $hashName for $inputPath")
      
        val randomVectors = Seq.fill(numOfRandomVectors)(DenseVector.fill(dimension)(Random.nextDouble))
        dbRandomVectors.put(hashName, randomVectors.map(_.data).toArray)
      
        dbSecionSizes.put(hashName, sectionSize)
      
        val hashTable = db.hashMap[String, Array[Long]](hashName)
      
        val allSmallBucket = dbVectors.entrySet.asScala
          .map(e => e.getKey -> e.getValue)
          .map {
            case (id, arr) =>
              val v = DenseVector(arr)
              val hash = randomVectors.map(rv => ((v dot rv) / sectionSize).floor.toInt).mkString("|")
              (id, hash)
          }
          .groupBy(_._2)
          .mapValues(_.map(_._1))
          .forall {
            case (hash, ids) =>
              if (ids.size <= 10000 || level == maxLevel) {
                hashTable.put(hash, ids.toArray)
                true
              } else {
                false
              }
          }
      
        db.commit()
      
        if (allSmallBucket) level else computeHash(level + 1, numOfRandomVectors + 3, sectionSize * 0.5)
      }
      computeHash(1, 3, 0.5)
    }

    (0 until numOfLsh).foreach(i => computeHashs(i))

    db.close()
    Logger.debug(s"$inputPath done")
  }
}
