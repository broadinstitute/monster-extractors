package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.Chain
import cats.effect.{ContextShift, IO}
import org.broadinstitute.monster.storage.gcs.GcsApi
import fs2.{Pull, Stream}
import org.http4s.{Charset, MediaType}
import org.http4s.headers.`Content-Type`
import de.odysseus.staxon.json.{JsonXMLConfigBuilder, JsonXMLOutputFactory}
import javax.xml.stream.XMLInputFactory._
import javax.xml.stream.events.XMLEvent
import cats.implicits._

import scala.collection.Iterator

class XmlExtractor(api: GcsApi)(implicit context: ContextShift[IO]) {

  def createJson(
    xmlTag: String,
    tagsPerFile: Long,
    inputBucket: String,
    inputPath: String,
    outputBucket: String,
    outputPath: String
  ): IO[Unit] = {
    val xml = api.readObject(inputBucket, inputPath, 0L)
    xmlToJson(xml, xmlTag, tagsPerFile).evalMap { jsonFile =>
      IO.pure(jsonFile)
        .bracket(
          file =>
            api.createObject(
              outputBucket,
              outputPath,
              `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
              None,
              Stream.emits(file.byteArray).covary[IO]
            )
        ) { file =>
          IO.delay(file.delete())
        }
    }
  }.compile.drain

  private def xmlToJson(
    xml: Stream[IO, Byte],
    xmlTag: String,
    tagsPerFile: Long
  ): Stream[IO, File] = {
    val jsonXMLConfig = new JsonXMLConfigBuilder()
      .autoArray(true)
      .autoPrimitive(true)
      .build()

    def xmlEventToFile(
      xmlEventStream: Stream[IO, XMLEvent],
      numberTagsAccumulated: Long,
      eventsAccumulated: Chain[XMLEvent]
    ): Pull[IO, Chain[XMLEvent], Unit] = {
      xmlEventStream.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((xmlEvent, remainingXmlEvents)) =>
          if (xmlEvent.isEndElement && xmlEvent
                .asEndElement()
                .getName
                .getLocalPart == xmlTag) {
            if (numberTagsAccumulated == tagsPerFile) {
              Pull.output1(eventsAccumulated.append(xmlEvent)).flatMap { _ =>
                xmlEventToFile(remainingXmlEvents, 0, Chain.empty)
              }
            } else {
              xmlEventToFile(
                remainingXmlEvents,
                numberTagsAccumulated + 1,
                eventsAccumulated.append(xmlEvent)
              )
            }
          } else {
            xmlEventToFile(
              remainingXmlEvents,
              numberTagsAccumulated,
              eventsAccumulated.append(xmlEvent)
            )
          }
      }
    }

    val xmlEventStream = xml.through(fs2.io.toInputStream).flatMap { inputStream =>
      val reader = newInstance().createXMLEventReader(inputStream)
      Stream.fromIterator[IO, XMLEvent](new Iterator[XMLEvent] {
        override def hasNext: Boolean = reader.hasNext
        override def next(): XMLEvent = reader.nextEvent()
      })
    }

    xmlEventToFile(xmlEventStream, 0, Chain.empty).stream.evalMap { xmlEvents =>
      IO.delay(File.newTemporaryFile()).flatMap { file =>
        IO.delay {
          new JsonXMLOutputFactory(jsonXMLConfig)
            .createXMLEventWriter(file.newOutputStream)
        }.bracket { writer =>
          xmlEvents.traverse { xmlEvent =>
            IO.delay(writer.add(xmlEvent))
          }.as(file)
        } { writer =>
          IO.delay(writer.close())
        }

      }
    }
  }
}
