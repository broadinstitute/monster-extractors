package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.Chain
import cats.effect.{ContextShift, IO}
import fs2.{Pipe, Pull, Stream}
import de.odysseus.staxon.json.{JsonXMLConfigBuilder, JsonXMLOutputFactory}
import javax.xml.stream.XMLInputFactory._
import javax.xml.stream.events.XMLEvent
import cats.implicits._

import scala.collection.Iterator

class XmlExtractor private[xml] (
  getXml: XmlExtractor.GcsObject => Stream[IO, Byte],
  writeJson: (File, XmlExtractor.GcsObject) => IO[Unit]
)(implicit context: ContextShift[IO]) {
  import XmlExtractor.GcsObject

  def extract(
    input: GcsObject,
    output: GcsObject,
    xmlTag: String,
    tagsPerFile: Long
  ): IO[Unit] = {
    getXml(input).through(xmlToJson(xmlTag, tagsPerFile)).evalMap { jsonFile =>
      IO.pure(jsonFile).bracket(writeJson(_, output)) { file =>
        IO.delay(file.delete())
      }
    }
  }.compile.drain

  private def xmlToJson(
    xmlTag: String,
    tagsPerFile: Long
  ): Pipe[IO, Byte, File] = { xml =>
    val jsonXMLConfig = new JsonXMLConfigBuilder()
      .autoArray(true)
      .autoPrimitive(true)
      .build()

    def chunkXmlTags(
      xmlEventStream: Stream[IO, XMLEvent],
      numberTagsAccumulated: Long,
      eventsAccumulated: Chain[XMLEvent]
    ): Pull[IO, Chain[XMLEvent], Unit] = {
      xmlEventStream.pull.uncons1.flatMap {
        case None => Pull.output1(eventsAccumulated)
        case Some((xmlEvent, remainingXmlEvents)) =>
          val newEvents = eventsAccumulated.append(xmlEvent)

          if (xmlEvent.isEndElement && xmlEvent
                .asEndElement()
                .getName
                .getLocalPart == xmlTag) {

            val newCount = numberTagsAccumulated

            if (newCount == tagsPerFile) {
              Pull.output1(newEvents).flatMap { _ =>
                chunkXmlTags(remainingXmlEvents, 0, Chain.empty)
              }
            } else {
              chunkXmlTags(
                remainingXmlEvents,
                newCount,
                newEvents
              )
            }
          } else {
            chunkXmlTags(
              remainingXmlEvents,
              numberTagsAccumulated,
              newEvents
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

    chunkXmlTags(xmlEventStream, 0, Chain.empty).stream.evalMap { xmlEvents =>
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

object XmlExtractor {
  case class GcsObject(bucket: String, path: String)
}
