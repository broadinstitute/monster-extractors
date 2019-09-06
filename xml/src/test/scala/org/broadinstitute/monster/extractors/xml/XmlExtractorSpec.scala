package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.effect.{ContextShift, IO}
import fs2.Stream
import io.circe.Json
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class XmlExtractorSpec extends FlatSpec with Matchers with EitherValues {

  behavior of "XmlExtractor"

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def readLocal(filename: String) = {
    val localFile = File("src/test/resources", filename)
    fs2.io.file.readAll[IO](localFile.path, ExecutionContext.global, 8196)
  }

  private def extractor(
    buffer: mutable.ArrayBuffer[(String, List[Json])]
  ): XmlExtractor[String] =
    new XmlExtractor(
      readLocal,
      (tmpFile, targetName, _) =>
        Stream
          .eval(IO.delay(tmpFile.lines.toSeq))
          .flatMap(Stream.emits)
          .map(io.circe.parser.parse)
          .rethrow
          .compile
          .toList
          .flatMap(jsons => IO.delay(buffer.append(targetName -> jsons)))
    )

  def conversionTest(
    description: String,
    filename: String,
    tagsPerFile: Int,
    expectedParts: List[(String, Long)]
  ): Unit = {
    it should description in {
      val buf = new mutable.ArrayBuffer[(String, List[Json])]()
      val in = s"inputs/$filename.xml"
      val out = s"outputs/$filename.json"

      // Extract JSONs into memory.
      extractor(buf).extract(in, out, tagsPerFile).unsafeRunSync()

      val expectedJsons = if (buf.isEmpty) {
        Nil
      } else {
        readLocal(out)
          .through(fs2.text.utf8Decode)
          .through(fs2.text.lines)
          .filter(!_.isEmpty)
          .map(io.circe.parser.parse)
          .rethrow
          .compile
          .toList
          .unsafeRunSync()
      }

      // Make sure expected objects were divided between output files as expected.
      buf.map { case (tag, jsons) => tag -> jsons.length } should contain theSameElementsInOrderAs expectedParts

      // Re-combine parts to make comparison simpler.
      val allOutputJsons = buf.foldLeft(List.empty[Json]) {
        case (acc, (_, jsons)) => jsons ::: acc
      }

      allOutputJsons should contain theSameElementsAs expectedJsons
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
