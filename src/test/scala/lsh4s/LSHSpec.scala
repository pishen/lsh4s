package lsh4s

import org.scalatest._
import better.files._

class LSHSpec extends FlatSpec with Matchers {
  def getEuclideanInputFile = {
    val inputFile = File.newTemporaryFile()
    inputFile.toJava.deleteOnExit()
    inputFile.appendLines(Seq(
      "10 1.0 1.0",
      "11 2.0 1.0",
      "12 2.0 2.0",
      "13 3.0 2.0"
    ): _*)

    inputFile
  }

  def checkEuclideanResult(res: Option[Seq[QueryResult]]) = {
    res.get.map(_.id) shouldBe Seq(11, 12, 13)
    res.get.map(_.distance).zip(Seq(1.0, 1.414, 2.236)).foreach {
      case (a, b) => a shouldBe b +- 0.001f
    }
  }

  "LSH" should "create/load and query Euclidean file index" in {
    val inputFile = getEuclideanInputFile

    val outputDir = File.newTemporaryDirectory()

    val lsh = LSH.hash(inputFile.pathAsString, 10, 10, (outputDir / "hash").pathAsString)

    checkEuclideanResult(lsh.query(10, 4))
    
    lsh.close()

    val lshReloaded = LSH.readLSH((outputDir / "hash").pathAsString)

    checkEuclideanResult(lshReloaded.query(10, 4))
    
    lshReloaded.close()
    outputDir.delete()
  }

  it should "create and query Euclidean memory index" in {
    val inputFile = getEuclideanInputFile

    val lsh = LSH.hash(inputFile.pathAsString, 10, 10, "mem")

    checkEuclideanResult(lsh.query(10, 4))
  }
}
