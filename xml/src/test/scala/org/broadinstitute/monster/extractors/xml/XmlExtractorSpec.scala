package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class XmlExtractorSpec extends FlatSpec with Matchers with EitherValues {
  import XmlExtractor.GcsObject

  behavior of "XmlExtractor"

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def readLocal(obj: GcsObject) = {
    val localFile = File("src/test/resources", obj.bucket, obj.path)
    fs2.io.file.readAll[IO](localFile.path, ExecutionContext.global, 8196)
  }

  private def extractor(buffer: mutable.ArrayBuffer[(String, GcsObject)]): XmlExtractor =
    new XmlExtractor(
      readLocal,
      (tmpFile, out) => IO.delay(buffer.append(new String(tmpFile.byteArray) -> out))
    )

  def conversionTest(description: String, filename: String): Unit = {
    it should description in {
      val buf = new mutable.ArrayBuffer[(String, GcsObject)]()
      val in = GcsObject("inputs", s"$filename.xml")
      val out = GcsObject("outputs", s"$filename.json")

      extractor(buf).extract(in, out, "Test", 6).unsafeRunSync()

      buf should have length 1L
      val (text, actualOut) = buf.head
      val outputJsons = text.lines.toList.traverse(io.circe.parser.parse).right.value
      val expectedJsons = readLocal(out)
        .through(fs2.text.utf8Decode)
        .through(fs2.text.lines)
        .filter(!_.isEmpty)
        .map(io.circe.parser.parse)
        .rethrow
        .compile
        .toList
        .unsafeRunSync()
      outputJsons should contain theSameElementsAs expectedJsons
      actualOut shouldBe out
    }
  }

  it should behave like conversionTest("convert XML to JSON", "simple")
  it should behave like conversionTest(
    "convert repeated top-level tags into repeated objects",
    "many-tags"
  )
  it should behave like conversionTest(
    "convert nested tags into nested objects",
    "nested"
  )
  it should behave like conversionTest(
    "convert repeated nested tags into arrays",
    "nested-repeated"
  )
}
