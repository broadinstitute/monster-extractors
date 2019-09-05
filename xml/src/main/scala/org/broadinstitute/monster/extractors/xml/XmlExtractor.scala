package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.Chain
import cats.effect.{ContextShift, IO, Resource}
import fs2.{Pipe, Pull, Stream}
import de.odysseus.staxon.json.{JsonXMLConfigBuilder, JsonXMLOutputFactory}
import javax.xml.stream.XMLInputFactory._
import javax.xml.stream.events.{StartElement, XMLEvent}
import cats.implicits._
import de.odysseus.staxon.event.SimpleXMLEventFactory
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.collection.Iterator
import scala.collection.JavaConverters._

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
  import XmlExtractor._

  private val log = Slf4jLogger.getLogger[IO]

  def extract(input: Path, output: Path, xmlTag: String, tagsPerFile: Int): IO[Unit] = {
    getXml(input)
      .through(parseXmlTags(xmlTag))
      .through(writeXmlAsJson(xmlTag, tagsPerFile))
      .zipWithIndex
      .evalMap {
        case (jsonFile, partNumber) =>
          log
            .info(s"Writing part #$partNumber to $output...")
            .flatMap { _ =>
              writeJson(jsonFile, partNumber, output).guarantee(
                IO.delay(jsonFile.delete())
              )
            }
            .as(partNumber + 1)
      }
  }.compile.lastOrError.flatMap { numParts =>
    log.info(s"Wrote $numParts parts to $output")
  }

  /**
    * Build a pipe which will convert a stream of raw bytes into a stream of
    * XML chunks, where each chunk represents a single instance of an XML tag.
    *
    * @param xmlTag the name of the tag to chunk on
    */
  private def parseXmlTags(xmlTag: String): Pipe[IO, Byte, Chain[XMLEvent]] = { xml =>
    /*
     * Helper for recursive chunking.
     *
     * @param rootTag the root of the entire XML document. XML roots are stripped by chunking,
     *        but we preserve their attributes in the top-level objects of our output
     * @param xmlEventStream un-chunked XML events
     * @param eventsAccumulated XML tags collected since reading the start of an `xmlTag`,
     *        before reading the corresponding end to the tag
     * @param inTag whether or not the head of `xmlEventStream` is nested within an `xmlTag`
     *              instance within the source document
     */
    def groupXmlTags(
      rootTag: StartElement,
      xmlEventStream: Stream[IO, XMLEvent],
      eventsAccumulated: Chain[XMLEvent],
      inTag: Boolean
    ): Pull[IO, Chain[XMLEvent], Unit] =
      xmlEventStream.pull.uncons1.flatMap {
        // No remaining events, exit the recursive loop.
        case None => Pull.done

        // Some number of events remaining.
        case Some((xmlEvent, remainingXmlEvents)) =>
          if (xmlEvent.isEndElement && xmlEvent
                .asEndElement()
                .getName
                .getLocalPart == xmlTag) {
            // End of an `xmlTag`, push a new chunk out of the stream.
            Pull.output1(eventsAccumulated.append(xmlEvent)).flatMap { _ =>
              // Recur with an empty accumulator.
              groupXmlTags(rootTag, remainingXmlEvents, Chain.empty, inTag = false)
            }
          } else if (xmlEvent.isStartElement && xmlEvent
                       .asStartElement()
                       .getName
                       .getLocalPart == xmlTag) {
            // Start of an `xmlTag`. Build a new start element containing
            // attributes from both this tag and the root element.
            val startElement = xmlEvent.asStartElement()
            val allAttributes: Iterator[Any] =
              startElement.getAttributes.asScala ++ rootTag.getAttributes.asScala
            val allNamespaces: Iterator[Any] =
              startElement.getNamespaces.asScala ++ rootTag.getNamespaces.asScala
            val combinedElement = EventFactory.createStartElement(
              startElement.getName,
              allAttributes.asJava,
              allNamespaces.asJava
            )

            // Initialize the accumulator with the new synthetic start, and recur.
            groupXmlTags(
              rootTag,
              xmlEventStream,
              eventsAccumulated.append(combinedElement),
              inTag = true
            )
          } else if (inTag) {
            // Continue accumulating events until we find an end event.
            groupXmlTags(
              rootTag,
              xmlEventStream,
              eventsAccumulated.append(xmlEvent),
              inTag = true
            )
          } else {
            // Not within an instance of `xmlTag`, no point in accumulating the event.
            groupXmlTags(rootTag, remainingXmlEvents, eventsAccumulated, inTag)
          }
      }

    // Bridge Java's XML APIs into fs2's Stream implementation.
    val xmlEventStream = xml.through(fs2.io.toInputStream).flatMap { inputStream =>
      val reader = newInstance().createXMLEventReader(inputStream)
      Stream.fromIterator[IO, XMLEvent](new Iterator[XMLEvent] {
        override def hasNext: Boolean = reader.hasNext
        override def next(): XMLEvent = reader.nextEvent()
      })
    }

    // Pull the stream until we find the first start element, which is the document root.
    xmlEventStream
      .dropWhile(!_.isStartElement)
      .pull
      .uncons1
      .flatMap {
        // No root found (document is empty?)
        case None => Pull.done
        // Begin recursive parsing on the element after the root.
        case Some((rootTag, nestedEvents)) =>
          groupXmlTags(
            rootTag.asStartElement(),
            nestedEvents,
            Chain.empty,
            inTag = false
          )
      }
      .stream
  }

  /**
    * Build a pipe which will group individual collections of XML events into batches,
    * write the batches to temp files as JSON-list, and return pointers to those files.
    *
    * @param xmlTag tag to use as the "virtual root" when converting XML to JSON
    * @param tagsPerFile number of XML tags to include in each batch
    */
  private def writeXmlAsJson(
    xmlTag: String,
    tagsPerFile: Int
  ): Pipe[IO, Chain[XMLEvent], File] = { xmlTags =>
    val jsonXMLConfig = new JsonXMLConfigBuilder()
      .autoArray(true)
      .autoPrimitive(false)
      .virtualRoot(xmlTag)
      .build()

    xmlTags.chunkN(tagsPerFile).evalMap { xmlChunk =>
      IO.delay(File.newTemporaryFile()).flatMap { file =>
        Resource.fromAutoCloseable(IO.delay(file.newOutputStream)).use { outputStream =>
          xmlChunk.traverse { xmlEvents =>
            val createWriter = IO.delay(
              new JsonXMLOutputFactory(jsonXMLConfig).createXMLEventWriter(outputStream)
            )

            Resource
              .make(createWriter)(writer => IO.delay(writer.close()))
              .use { writer =>
                xmlEvents.traverse_(xmlEvent => IO.delay(writer.add(xmlEvent)))
              }
              .flatMap(_ => IO.delay(outputStream.write('\n')))
          }.as(file)
        }
      }
    }
  }
}

object XmlExtractor {

  /** Helper used to build modified XML events when needed. */
  private val EventFactory = new SimpleXMLEventFactory()
}
