package org.broadinstitute.monster.extractors.xml

import better.files._
import cats.data._
import cats.effect.{Blocker, ContextShift, IO}
import cats.implicits._
import fs2.{io => _, _}
import javax.xml.stream.events.{EndElement, StartElement, XMLEvent}
import com.ctc.wstx.stax.{WstxEventFactory, WstxInputFactory}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.codehaus.jettison.AbstractXMLEventWriter
import org.codehaus.jettison.badgerfish.BadgerFishXMLStreamWriter

import scala.collection.Iterator

/**
  * Utility which can mechanically convert a stream of XML to a stream of chunked JSON-list files.
  *
  * JSON-list files are temporarily stored on local disk in case the mechanism which writes
  * the data to its final storage space needs to know the total size of the file.
  */
class XmlExtractor private[xml] (blocker: Blocker)(implicit context: ContextShift[IO]) {
  private val log = Slf4jLogger.getLogger[IO]

  /** Helper used to build modified XML events when needed. */
  private val EventFactory = new WstxEventFactory()

  /** Helper used to build XML readers when needed. */
  private val ReaderFactory = new WstxInputFactory()

  /**
    * Extract an XML payload into a collection of JSON-list parts,
    * batching output JSON according to top-level tag and a max count.
    *
    * @param input pointer to the input XML payload
    * @param output pointer to the directory where JSON should be written
    * @param tagsPerFile maximum number of JSON objects to write to
    *                    each output file
    */
  def extract(
    input: File,
    output: File,
    tagsPerFile: Int,
    gunzip: Boolean
  ): Stream[IO, File] = {
    val baseBytes = fs2.io.file.readAll[IO](input.path, blocker, 8192)
    val xml = if (gunzip) baseBytes.through(fs2.compress.gunzip(2 * 8192)) else baseBytes

    xml
      .through(parseXmlTags)
      .through(batchTags(tagsPerFile))
      .through(writeJson(output))
  }

  /**
    * Build a pipe which will convert a stream of raw bytes into a stream of
    * XML chunks, where each chunk represents a single instance of an XML tag.
    */
  private val parseXmlTags: Pipe[IO, Byte, NonEmptyChain[XMLEvent]] = { xml =>
    // Bridge Java's XML APIs into fs2's Stream implementation.
    val xmlEventStream = xml.through(fs2.io.toInputStream).flatMap { inStream =>
      Stream.fromIterator[IO](new Iterator[XMLEvent] {
        private val underlying = ReaderFactory.createXMLEventReader(inStream)
        override def hasNext: Boolean = underlying.hasNext
        override def next(): XMLEvent = underlying.nextEvent()
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
          val rootStart = rootTag.asStartElement()
          val rootEnd =
            EventFactory.createEndElement(rootStart.getName, rootStart.getNamespaces)
          collectXmlTags((rootStart, rootEnd), nestedEvents, Chain.empty, None)
      }
      .stream
  }

  /**
    * Helper for recursive chunking.
    *
    * @param rootElements a start/end pair of tags for the root of the entire XML document.
    *                     XML roots are stripped by chunking, but we preserve their attributes
    *                     in the top-level objects of our output
    * @param xmlEventStream un-chunked XML events
    * @param eventsAccumulated XML tags collected since reading the start of an `xmlTag`,
    *        before reading the corresponding end to the tag
    * @param currentTag name of the top-level tag being collected within `eventsAccumulated`,
    *                   if any
    */
  private def collectXmlTags(
    rootElements: (StartElement, EndElement),
    xmlEventStream: Stream[IO, XMLEvent],
    eventsAccumulated: Chain[XMLEvent],
    currentTag: Option[String]
  ): Pull[IO, NonEmptyChain[XMLEvent], Unit] =
    xmlEventStream.pull.uncons1.flatMap {
      // No remaining events, exit the recursive loop.
      case None => Pull.done

      // Some number of events remaining.
      case Some((xmlEvent, remainingXmlEvents)) =>
        currentTag match {
          // Looking for a new root tag to begin accumulating.
          case None =>
            if (xmlEvent.isStartElement) {
              // Start of a top-level tag.
              val startElement = xmlEvent.asStartElement()
              // Recur with the start tag's name as the marker-to-accumulate.
              collectXmlTags(
                rootElements,
                xmlEventStream,
                // Inject the root-level tags as a nested object so we don't
                // lose any information when we output as flat JSON-list.
                Chain(startElement, rootElements._1, rootElements._2),
                currentTag = Some(startElement.getName.getLocalPart)
              )
            } else {
              // Not within a root-level tag, no point in accumulating the event.
              collectXmlTags(
                rootElements,
                remainingXmlEvents,
                eventsAccumulated,
                currentTag
              )
            }

          // Accumulating events under a tag.
          case Some(xmlTag) =>
            val newAccumulator = eventsAccumulated.append(xmlEvent)
            if (xmlEvent.isEndElement && xmlEvent
                  .asEndElement()
                  .getName
                  .getLocalPart == xmlTag) {
              // End of the accumulated tag. Output anything we've collected and
              // recur with an empty accumulator.
              val outputIfNonEmpty = NonEmptyChain
                .fromChain(newAccumulator)
                .fold[Pull[IO, NonEmptyChain[XMLEvent], Unit]](Pull.done)(Pull.output1)

              outputIfNonEmpty >> collectXmlTags(
                rootElements,
                remainingXmlEvents,
                Chain.empty,
                currentTag = None
              )
            } else {
              // Continue accumulating events.
              collectXmlTags(rootElements, remainingXmlEvents, newAccumulator, currentTag)
            }
        }
    }

  /**
    * Build a pipe which will convert a stream of XML tags into batches of tags,
    * paired with the name of the top-level start element common to every tag in the batch.
    *
    * @param maxSize max number of tags to include in a single output batch
    */
  private def batchTags(
    maxSize: Int
  ): Pipe[IO, NonEmptyChain[XMLEvent], (String, Chunk[NonEmptyChain[XMLEvent]])] =
    _.groupAdjacentByLimit(maxSize)(_.head.asStartElement().getName.getLocalPart)

  /**
    * Build a pipe which will write XML batches (grouped by element name) to disk
    * as JSON-list part-files under an output directory.
    *
    * @param out path to the directory where output part-files should be written.
    *            Files will be written at paths like `out`/`element-name`/`part-N.json`
    */
  private def writeJson(
    out: File
  ): Pipe[IO, (String, Chunk[NonEmptyChain[XMLEvent]]), File] =
    _.mapAccumulate(Map.empty[String, Long]) {
      case (partCounts, (tagName, xmlChonk)) =>
        val partNumber = partCounts.getOrElse(tagName, 0L) + 1L
        (partCounts.updated(tagName, partNumber), (tagName, xmlChonk, partNumber))
    }.evalMap {
      case (_, (tagName, xmlChonk, partNumber)) =>
        log
          .info(s"Writing part #$partNumber for $tagName to $out...")
          .flatMap { _ =>
            blocker.delay[IO, File] {
              val parentDir = (out / tagName).createDirectories()
              val outFile = parentDir / s"part-$partNumber.json"

              outFile.bufferedWriter.foreach { writer =>
                xmlChonk.foreach { tag =>
                  val jsonWriter = new BadgerFishXMLStreamWriter(writer)
                  val eventWriter = new AbstractXMLEventWriter(jsonWriter)
                  tag.iterator.foreach(eventWriter.add)
                  // Force the writer to output its accumulated state.
                  jsonWriter.writeEndDocument()
                  writer.write('\n')
                }
              }

              outFile
            }
          }
    }
}
