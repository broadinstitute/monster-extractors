package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.Chain
import cats.effect.{ContextShift, IO, Resource}
import fs2.{Pipe, Pull, Stream}
import de.odysseus.staxon.json.{JsonXMLConfigBuilder, JsonXMLOutputFactory}
import javax.xml.stream.XMLInputFactory._
import javax.xml.stream.events.XMLEvent
import cats.implicits._

import scala.collection.Iterator

/**
  * Utility which can mechanically convert a stream of XML to a stream of chunked JSON-list files.
  *
  * JSON-list files are temporarily stored on local disk in case the mechanism which writes
  * the data to its final storage space needs to know the total size of the file.
  *
  * @tparam Path type describing a location in the storage system where XML is hosted /
  *              JSON-list should be written
  *
  * @param getXml function which can produce a stream of XML from a location in storage
  * @param writeJson function which can copy a local temp file containing JSON-list data
  *                  to an external storage system
  */
class XmlExtractor[Path] private[xml] (
  getXml: Path => Stream[IO, Byte],
  writeJson: (File, Long, Path) => IO[Unit]
)(implicit context: ContextShift[IO]) {

  def extract(input: Path, output: Path, xmlTag: String, tagsPerFile: Int): IO[Unit] = {
    getXml(input).through(xmlToJson(xmlTag, tagsPerFile)).zipWithIndex.evalMap {
      case (jsonFile, partNumber) =>
        writeJson(jsonFile, partNumber, output).guarantee(IO.delay(jsonFile.delete()))
    }
  }.compile.drain

  private def xmlToJson(xmlTag: String, tagsPerFile: Int): Pipe[IO, Byte, File] = { xml =>
    val jsonXMLConfig = new JsonXMLConfigBuilder()
      .autoArray(true)
      .autoPrimitive(false)
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
          Resource.make {
            IO.delay(file.newOutputStream)
          } { outputStream =>
            IO.delay(outputStream.close())
          }.use { outputStream =>
            xmlChunk.traverse { xmlEvents =>
              Resource.make {
                IO.delay {
                  new JsonXMLOutputFactory(jsonXMLConfig)
                    .createXMLEventWriter(outputStream)
                }
              } { writer =>
                IO.delay {
                  writer.close()
                }
              }.use { writer =>
                xmlEvents.traverse_ { xmlEvent =>
                  IO.delay {
                    writer.add(xmlEvent)
                  }
                }
              }.flatMap { _ =>
                IO.delay(outputStream.write('\n'))
              }
            }.as(file)
          }
        }
      }
  }
}
