package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.effect.{ContextShift, IO}
import cats.implicits._
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
    buffer: mutable.ArrayBuffer[(String, Long)]
  ): XmlExtractor[String] =
    new XmlExtractor(
      readLocal,
      (tmpFile, partNumber, _) =>
        IO.delay(buffer.append(new String(tmpFile.byteArray) -> partNumber))
    )

  def conversionTest(
    description: String,
    filename: String,
    tagsPerFile: Int = 1
  ): Unit = {
    it should description in {
      val buf = new mutable.ArrayBuffer[(String, Long)]()
      val in = s"inputs/$filename.xml"
      val out = s"outputs/$filename.json"

      // Extract JSONs into memory.
      extractor(buf).extract(in, out, "Test", tagsPerFile).unsafeRunSync()

      val expectedJsons = readLocal(out)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .filter(!_.isEmpty)
        .map(io.circe.parser.parse)
        .rethrow
        .compile
        .toList
        .unsafeRunSync()

      // Make sure expected objects were divided between output files as expected.
      buf should have length (expectedJsons.length / tagsPerFile).toLong

      // Re-combine parts to make comparison simpler.
      val allOutputJsons = buf.foldLeft(List.empty[Json]) {
        case (acc, (text, _)) =>
          text.lines.toList.traverse(io.circe.parser.parse).right.value ::: acc
      }

      allOutputJsons should contain theSameElementsAs expectedJsons
    }
  }

  it should behave like conversionTest("convert XML to JSON", "simple")
  it should behave like conversionTest(
    "convert repeated top-level tags into repeated objects",
    "many-tags"
  )
  it should behave like conversionTest(
    "support user-specified chunk counts for top-level repeated objects",
    "many-tags",
    tagsPerFile = 2
  )
  it should behave like conversionTest(
    "convert nested tags into nested objects",
    "nested"
  )
  it should behave like conversionTest(
    "convert repeated nested tags into arrays",
    "nested-repeated"
  )
  it should behave like conversionTest(
    "include root-level attributes in top-level objects",
    "root-attributes"
  )
}
