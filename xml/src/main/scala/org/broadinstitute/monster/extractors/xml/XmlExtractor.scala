package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.Chain
import cats.effect.{ContextShift, IO, Resource}
import fs2.{Chunk, Pipe, Pull, Stream}
import de.odysseus.staxon.json.{JsonXMLConfigBuilder, JsonXMLOutputFactory}
import javax.xml.stream.XMLInputFactory._
import javax.xml.stream.events.{StartElement, XMLEvent}
import cats.implicits._
import de.odysseus.staxon.event.SimpleXMLEventFactory
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.annotation.tailrec
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
  writeJson: (File, String, Path) => IO[Unit]
)(implicit context: ContextShift[IO]) {
  import XmlExtractor._

  private val log = Slf4jLogger.getLogger[IO]

  def extract(input: Path, output: Path, tagsPerFile: Int): IO[Unit] = {
    getXml(input)
      .through(parseXmlTags)
      .through(writeXmlAsJson(tagsPerFile))
      .evalScan(Map.empty[String, Long]) {
        case (partCounts, (tagName, jsonFile)) =>
          val partNumber = partCounts.getOrElse(tagName, 0L) + 1L
          for {
            _ <- log.info(s"Writing part #$partNumber for $tagName to $output...")
            _ <- writeJson(jsonFile, s"$tagName/part-$partNumber.json", output).guarantee(
              IO.delay(jsonFile.delete())
            )
          } yield {
            partCounts.updated(tagName, partNumber)
          }
      }
  }.compile.last.flatMap {
    case None =>
      log.warn(s"No extractable data found, nothing written to $output")
    case Some(finalCounts) =>
      finalCounts.toList.traverse_ {
        case (tag, count) =>
          log.info(s"Wrote $count parts to $output for tag $tag")
      }
  }

  /**
    * Build a pipe which will convert a stream of raw bytes into a stream of
    * XML chunks, where each chunk represents a single instance of an XML tag.
    */
  private val parseXmlTags: Pipe[IO, Byte, Tag] = { xml =>
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
          groupXmlTags(rootTag.asStartElement(), nestedEvents, Chain.empty, None)
      }
      .stream
  }

  /**
    * Helper for recursive chunking.
    *
    * @param rootTag the root of the entire XML document. XML roots are stripped by chunking,
    *        but we preserve their attributes in the top-level objects of our output
    * @param xmlEventStream un-chunked XML events
    * @param eventsAccumulated XML tags collected since reading the start of an `xmlTag`,
    *        before reading the corresponding end to the tag
    * @param currentTag TODO
    */
  private def groupXmlTags(
    rootTag: StartElement,
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
              // Start of a top-level tag. Build a new start element containing
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
                currentTag = Some(startElement.getName.getLocalPart)
              )
            } else {
              // Not within a root-level tag, no point in accumulating the event.
              groupXmlTags(rootTag, remainingXmlEvents, eventsAccumulated, currentTag)
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
                groupXmlTags(rootTag, remainingXmlEvents, Chain.empty, currentTag = None)
              }
            } else {
              // Continue accumulating events.
              groupXmlTags(rootTag, remainingXmlEvents, newAccumulator, currentTag)
            }
        }
    }

  /**
    * Build a pipe which will group individual collections of XML events into batches,
    * write the batches to temp files as JSON-list, and return pointers to those files.
    *
    * @param maxTagsPerFile maximum number of XML tags to write as JSON in a file
    */
  private def writeXmlAsJson(maxTagsPerFile: Int): Pipe[IO, Tag, (String, File)] = {
    tags =>
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
        if (differsAt == -1 && totalSize + chunkSize <= maxTagsPerFile) {
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
          val canFillGroup = differsAt == -1 || totalSize + differsAt > maxTagsPerFile
          val finalIndex = if (canFillGroup) {
            // EITHER:
            //   1. Whole chunk matches the current key, but there are too many elements
            //      to store in a single chunk.
            //   2. The first element tag with a different name is "far enough" away from
            //      the head of the chunk that we'll reach the max-tag threshold before we
            //      reach it.
            maxTagsPerFile - totalSize
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

      groupAdjacentByTag(None, namedTags).stream.evalMap {
        case (name, xmlChunk) =>
          val jsonXMLConfig = new JsonXMLConfigBuilder()
            .autoArray(true)
            .autoPrimitive(false)
            .virtualRoot(name)
            .build()

          IO.delay(File.newTemporaryFile()).flatMap { file =>
            Resource.fromAutoCloseable(IO.delay(file.newOutputStream)).use {
              outputStream =>
                xmlChunk.traverse { xmlEvents =>
                  val createWriter = IO.delay(
                    new JsonXMLOutputFactory(jsonXMLConfig)
                      .createXMLEventWriter(outputStream)
                  )

                  Resource
                    .make(createWriter)(writer => IO.delay(writer.close()))
                    .use { writer =>
                      xmlEvents.traverse_(xmlEvent => IO.delay(writer.add(xmlEvent)))
                    }
                    .flatMap(_ => IO.delay(outputStream.write('\n')))
                }.as(name -> file)
            }
          }
      }
  }
}

object XmlExtractor {

  private type Tag = Chain[XMLEvent]

  /** Helper used to build modified XML events when needed. */
  private val EventFactory = new SimpleXMLEventFactory()
}
