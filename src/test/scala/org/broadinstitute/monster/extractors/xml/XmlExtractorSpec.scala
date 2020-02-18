package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.effect.{Blocker, ContextShift, IO}
import cats.implicits._
import io.circe.Json
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class XmlExtractorSpec extends AnyFlatSpec with Matchers with EitherValues {
  behavior of "XmlExtractor"

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private val blocker = Blocker.liftExecutionContext(ExecutionContext.global)

  private def extractor: XmlExtractor = new XmlExtractor(blocker)

  private def readJsons(file: File): List[Json] =
    file.lines
      .filter(!_.isEmpty)
      .toList
      .traverse(io.circe.parser.parse)
      .right
      .value

  def conversionTest(
    description: String,
    filename: String,
    tagsPerFile: Int,
    expectedParts: List[(String, Long)]
  ): Unit = {
    it should description in {
      val input = File(s"src/test/resources/inputs/$filename.xml")
      val expectedJsons = readJsons(File(s"src/test/resources/outputs/$filename.json"))

      File.temporaryDirectory() { tmpOut =>
        // Extract JSONs into memory.
        val jsonsByFilename = extractor
          .extract(input, tmpOut, tagsPerFile, gunzip = false)
          .map { outFile =>
            val relativeName = tmpOut.relativize(outFile).toString
            relativeName -> readJsons(outFile)
          }
          .compile
          .toList
          .unsafeRunSync()

        // Make sure expected objects were divided between output files as expected.
        jsonsByFilename.map {
          case (tag, jsons) => tag -> jsons.length
        } should contain theSameElementsInOrderAs expectedParts

        // Re-combine parts to make comparison simpler.
        val allOutputJsons = jsonsByFilename.foldLeft(List.empty[Json]) {
          case (acc, (_, jsons)) => jsons ::: acc
        }

        allOutputJsons should contain theSameElementsAs expectedJsons
      }
    }
  }

  it should behave like conversionTest(
    "convert XML to JSON",
    "simple",
    tagsPerFile = 1,
    expectedParts = List("Test/part-1.json" -> 1)
  )
  it should behave like conversionTest(
    "convert repeated top-level tags into repeated objects",
    "many-tags",
    tagsPerFile = 1,
    expectedParts = List("Test/part-1.json" -> 1, "Test/part-2.json" -> 1)
  )
  it should behave like conversionTest(
    "support user-specified chunk counts for top-level repeated objects",
    "many-tags",
    tagsPerFile = 2,
    expectedParts = List("Test/part-1.json" -> 2)
  )
  it should behave like conversionTest(
    "terminate if there are fewer tags in the document than the max count",
    "many-tags",
    tagsPerFile = 100,
    expectedParts = List("Test/part-1.json" -> 2)
  )
  it should behave like conversionTest(
    "convert nested tags into nested objects",
    "nested",
    tagsPerFile = 1,
    expectedParts = List("Test/part-1.json" -> 1)
  )
  it should behave like conversionTest(
    "convert repeated nested tags into arrays",
    "nested-repeated",
    tagsPerFile = 1,
    expectedParts = List("Test/part-1.json" -> 1)
  )
  it should behave like conversionTest(
    "include root-level attributes in top-level objects",
    "root-attributes",
    tagsPerFile = 2,
    expectedParts = List("Test/part-1.json" -> 2)
  )
  it should behave like conversionTest(
    "not write output if there is no XML to extract",
    "empty",
    tagsPerFile = 1,
    expectedParts = Nil
  )
  it should behave like conversionTest(
    "handle multiple top-level tag types in one document",
    "mixed-tags",
    tagsPerFile = 2,
    expectedParts = List(
      "Test/part-1.json" -> 2,
      "Test2/part-1.json" -> 1,
      "Test/part-2.json" -> 1,
      "Test2/part-2.json" -> 2,
      "Test2/part-3.json" -> 1,
      "Test3/part-1.json" -> 1
    )
  )
}
