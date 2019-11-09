package org.broadinstitute.monster.extractors.xml

import better.files._
import cats.data.Chain
import cats.effect.{Blocker, ContextShift, IO}
import fs2.{io => _, _}
import javax.xml.stream.events.{EndElement, StartElement, XMLEvent}
import cats.implicits._
import com.ctc.wstx.stax.{WstxEventFactory, WstxInputFactory}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.codehaus.jettison.AbstractXMLEventWriter
import org.codehaus.jettison.badgerfish.BadgerFishXMLStreamWriter

import scala.annotation.tailrec
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

  /** TODO */
  private val ReaderFactory = new WstxInputFactory()

  private type Tag = Chain[XMLEvent]

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
  ): Stream[IO, (String, File)] = {
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
  private val parseXmlTags: Pipe[IO, Byte, Tag] = { xml =>
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
  ): Pull[IO, Tag, Unit] =
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
              // End of the accumulated tag, push a new chunk out of the stream.
              Pull.output1(newAccumulator).flatMap { _ =>
                // Recur with an empty accumulator.
                collectXmlTags(
                  rootElements,
                  remainingXmlEvents,
                  Chain.empty,
                  currentTag = None
                )
              }
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
  private def batchTags(maxSize: Int): Pipe[IO, Tag, (String, Chunk[Tag])] = { tags =>
    /*
     * NOTE: The logic in these two definitions is almost entirely lifted from
     * the body of `groupAdjacentBy` in fs2's Stream implementation, with:
     *   1. (Hopeful) readability improvements for our specific case
     *   2. Some adjustments to account for a max size on the output groups
     *
     * https://github.com/functional-streams-for-scala/fs2/issues/1588 tracks
     * patching the "max size" option back into fs2.
     */
    def groupAdjacentByTag(
      currentGroup: Option[(String, Chunk[Tag])],
      tagStream: Stream[IO, (String, Tag)]
    ): Pull[IO, (String, Chunk[Tag]), Unit] =
      tagStream.pull.uncons.flatMap {
        case Some((nextChunk, remainingTags)) =>
          if (nextChunk.nonEmpty) {
            val (tagName, out) =
              currentGroup.getOrElse(nextChunk(0)._1 -> Chunk.empty[Tag])
            rechunk(nextChunk, remainingTags, tagName, List(out), out.size, None)
          } else {
            groupAdjacentByTag(currentGroup, remainingTags)
          }
        case None =>
          currentGroup.map(Pull.output1).getOrElse(Pull.done)
      }

    @tailrec
    def rechunk(
      nextChunk: Chunk[(String, Tag)],
      tagStream: Stream[IO, (String, Tag)],
      currentTag: String,
      out: List[Chunk[Tag]],
      totalSize: Int,
      acc: Option[Chunk[(String, Chunk[Tag])]]
    ): Pull[IO, (String, Chunk[Tag]), Unit] = {
      val chunkSize = nextChunk.size
      val differsAt = nextChunk.indexWhere(_._1 != currentTag).getOrElse(-1)
      if (differsAt == -1 && totalSize + chunkSize <= maxSize) {
        // Whole chunk matches the current key, add this chunk to the accumulated output.
        val newOut = Chunk.concat((nextChunk.map(_._2) :: out).reverse)
        acc match {
          case None =>
            groupAdjacentByTag(Some(currentTag -> newOut), tagStream)
          case Some(acc) =>
            // Potentially outputs one additional chunk (by splitting the last one in two)
            Pull.output(acc) >>
              groupAdjacentByTag(Some(currentTag -> newOut), tagStream)
        }
      } else {
        val canFillGroup = differsAt == -1 || totalSize + differsAt > maxSize
        val finalIndex = if (canFillGroup) {
          // EITHER:
          //   1. Whole chunk matches the current key, but there are too many elements
          //      to store in a single chunk.
          //   2. The first element tag with a different name is "far enough" away from
          //      the head of the chunk that we'll reach the max-tag threshold before we
          //      reach it.
          maxSize - totalSize
        } else {
          // The first element tag with a different name is "close enough" to the head
          // of the chunk that we can add all the remaining same-named tags to the current
          // accumulator without passing the size threshold.
          differsAt
        }

        val included = nextChunk.map(_._2).take(finalIndex)
        val excluded = nextChunk.drop(finalIndex)

        val nextTag = if (canFillGroup) {
          // Begin building a new chunk for the same tag.
          currentTag
        } else {
          // Begin building a chunk for a new tag.
          excluded(0)._1
        }
        val nextOut = Chunk.concat((included :: out).reverse)
        val nextAcc = Chunk.concat(acc.toList ::: List(Chunk(currentTag -> nextOut)))
        rechunk(nextChunk, tagStream, nextTag, Nil, 0, Some(nextAcc))
      }
    }

    val namedTags = tags.mapFilter { tag =>
      tag.headOption.map(_.asStartElement().getName.getLocalPart -> tag)
    }

    groupAdjacentByTag(None, namedTags).stream
  }

  /** TODO */
  private def writeJson(out: File): Pipe[IO, (String, Chunk[Tag]), (String, File)] =
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
          .map(tagName -> _)
    }
}
