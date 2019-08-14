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
    tagsPerFile: Int
  ): IO[Unit] = {
    getXml(input).through(xmlToJson(xmlTag, tagsPerFile)).evalMap { jsonFile =>
      IO.pure(jsonFile).bracket(writeJson(_, output)) { file =>
        IO.delay(file.delete())
      }
    }
  }.compile.drain

  private def xmlToJson(xmlTag: String, tagsPerFile: Int): Pipe[IO, Byte, File] = { xml =>
    val jsonXMLConfig = new JsonXMLConfigBuilder()
      .autoArray(true)
      .autoPrimitive(true)
      .repairingNamespaces(true)
      .virtualRoot(xmlTag)
      .build()

    def groupXmlTags(
      xmlEventStream: Stream[IO, XMLEvent],
      eventsAccumulated: Chain[XMLEvent],
      inTag: Boolean
    ): Pull[IO, Chain[XMLEvent], Unit] = {
      xmlEventStream.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((xmlEvent, remainingXmlEvents)) =>
          if (xmlEvent.isEndElement && xmlEvent
                .asEndElement()
                .getName
                .getLocalPart == xmlTag) {
            Pull.output1(eventsAccumulated.append(xmlEvent)).flatMap { _ =>
              groupXmlTags(remainingXmlEvents, Chain.empty, inTag = false)
            }
          } else if (inTag || xmlEvent.isStartElement && xmlEvent
                       .asStartElement()
                       .getName
                       .getLocalPart == xmlTag) {
            groupXmlTags(xmlEventStream, eventsAccumulated.append(xmlEvent), inTag = true)
          } else {
            groupXmlTags(remainingXmlEvents, eventsAccumulated, inTag)
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

    groupXmlTags(xmlEventStream, Chain.empty, inTag = false).stream
      .chunkN(tagsPerFile)
      .evalMap { xmlChunk =>
        IO.delay(File.newTemporaryFile()).flatMap { file =>
          xmlChunk.traverse { xmlEvents =>
            IO.delay {
              new JsonXMLOutputFactory(jsonXMLConfig)
                .createXMLEventWriter(file.newOutputStream(File.OpenOptions.append))
            }.bracket { writer =>
              xmlEvents.traverse { xmlEvent =>
                IO.delay(writer.add(xmlEvent))
              }
            } { writer =>
              IO.delay(writer.close())
            }
          }.as(file)
        }
      }
  }
}

object XmlExtractor {
  case class GcsObject(bucket: String, path: String)
}
