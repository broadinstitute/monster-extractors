package org.broadinstitute.monster.extractors.xml

import better.files.File
import cats.data.{Validated, ValidatedNel}
import cats.effect.{Blocker, ExitCode, IO}
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.effect._
import org.broadinstitute.monster.XmlClpBuildInfo

/**
  * Command-line program which can use our extractor functionality to convert
  * XML to chunked JSON on local disk.
  */
object ExtractorClp
    extends CommandIOApp(
      name = "xml-extract",
      header = "Mechanically extract XML data into JSON-list",
      version = XmlClpBuildInfo.version
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
        Blocker[IO].use { blocker =>
          new XmlExtractor(blocker)
            .extract(in, out, count, gunzip)
            .compile
            .last
            .map {
              case None    => ExitCode.Error
              case Some(_) => ExitCode.Success
            }
        }
    }
  }
}
