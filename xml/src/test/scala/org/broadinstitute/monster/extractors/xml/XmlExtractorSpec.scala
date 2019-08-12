package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.effect.{ContextShift, IO}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class XmlExtractorSpec extends FlatSpec with Matchers {
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

  it should "convert XML to JSON" in {
    val buf = new mutable.ArrayBuffer[(String, GcsObject)]()
    val in = GcsObject("bucket", "test.xml")
    val out = GcsObject("bucket2", "test.json")

    extractor(buf).extract(in, out, "Test", 6).unsafeRunSync()

    buf should have length 1L
    val (text, actualOut) = buf.head
    text shouldBe new String(readLocal(in).compile.toChunk.unsafeRunSync().toArray)
    actualOut shouldBe out
  }
}
