package org.broadinstitute.monster.extractors.xml

import java.util.concurrent.{ExecutorService, Executors}

import better.files.File
import buildinfo.BuildInfo
import cats.data.{Validated, ValidatedNel}
import cats.effect.{ExitCode, IO, Resource}
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.effect._

import scala.concurrent.ExecutionContext

/**
  * Command-line program which can use our extractor functionality to convert
  * XML to chunked JSON on local disk.
  */
object ExtractorClp
    extends CommandIOApp(
      name = "xml-extract",
      header = "Mechanically extract XML data into JSON-list",
      version = BuildInfo.version
    ) {

  implicit val fileArg: Argument[File] = new Argument[File] {
    override def read(string: String): ValidatedNel[String, File] =
      try {
        Validated.validNel(File(string))
      } catch {
        case e: Exception =>
          Validated.invalidNel(s"Failed to parse '$string' as a path: ${e.getMessage}")
      }

    override def defaultMetavar: String = "path"
  }

  override def main: Opts[IO[ExitCode]] = {
    val inputOpt = Opts
      .option[File](
        "input",
        "Path to the local XML file to extract.",
        short = "i"
      )
      .validate("Input does not exist")(_.isReadable)

    val outputOpt = Opts
      .option[File](
        "output",
        "Path to the local directory where extracted JSON should be written. " +
          "Will raise an error if pointed to an existing non-directory.",
        short = "o"
      )
      .validate("Output is not a directory") { out =>
        out.notExists || out.isDirectory
      }

    val gzipOpt = Opts.flag("gunzip-input", "Gunzip input before extraction").orFalse

    val countOpt = Opts.option[Int](
      "objects-per-part",
      "Number of JSON objects to write to each partfile in the output.",
      short = "n"
    )

    (inputOpt, outputOpt, gzipOpt, countOpt).mapN {
      case (in, out, gunzip, count) =>
        val blockingEc = {
          val allocate = IO.delay(Executors.newCachedThreadPool())
          val free = (es: ExecutorService) => IO.delay(es.shutdown())
          Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
        }

        blockingEc.use { ec =>
          val extractor = {
            val readPath = (inFile: File) => {
              val base = fs2.io.file.readAll[IO](inFile.path, ec, 8192)
              if (gunzip) base.through(fs2.compress.gunzip(2 * 8192)) else base
            }

            val writePath = (tmp: File, outSuffix: String, outPrefix: File) => {
              val outFile = outPrefix / outSuffix
              contextShift.evalOn(ec)(IO.delay(tmp.copyTo(outFile))).void
            }

            new XmlExtractor[File](readPath, writePath)
          }

          for {
            _ <- IO.delay(out.createDirectories())
            _ <- extractor.extract(in, out, count)
          } yield {
            ExitCode.Success
          }
        }
    }
  }
}
