package org.broadinstitute.monster.extractors.xml

import java.util.concurrent.Executors

import better.files.File
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.{Blocker, ExitCode, IO, Resource}
import cats.implicits._
import com.monovore.decline._
import com.monovore.decline.effect._
import org.broadinstitute.monster.MonsterXmlToJsonListBuildInfo

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
  * Command-line program which can use our extractor functionality to convert
  * XML to chunked JSON on local disk.
  */
object ExtractorClp
    extends CommandIOApp(
      name = "xml-extract",
      header = "Mechanically extract XML data into JSON-list",
      version = MonsterXmlToJsonListBuildInfo.version
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
        // CE's default Blocker implementation hangs on fatal errors (i.e. OOM).
        // Manually build a Blocker so we can inject the fix that's been merged,
        // but not released: https://github.com/typelevel/cats-effect/pull/694.
        val buildExecutorService =
          IO.delay(Executors.newCachedThreadPool { runnable =>
            val t = new Thread(runnable, "custom-blocker")
            t.setDaemon(true)
            t
          })
        Resource
          .make(buildExecutorService) { es =>
            val tasks = IO.delay {
              val tasks = es.shutdownNow()
              val builder = List.newBuilder[Runnable]
              val itr = tasks.iterator()
              while (itr.hasNext) {
                builder += itr.next()
              }
              NonEmptyList.fromList(builder.result)
            }
            tasks.flatMap {
              case Some(t) => IO.raiseError(new Blocker.OutstandingTasksAtShutdown(t))
              case None    => IO.unit
            }
          }
          .map { es =>
            // Delegate to the default EC, but intercept fatal errors.
            new ExecutionContext {
              private val base = ExecutionContext.fromExecutorService(es)
              override def execute(runnable: Runnable): Unit =
                base.execute { () =>
                  try {
                    runnable.run()
                  } catch {
                    case NonFatal(err) => reportFailure(err)
                    case t: Throwable =>
                      t.printStackTrace()
                      System.exit(1)
                  }
                }
              override def reportFailure(cause: Throwable): Unit =
                base.reportFailure(cause)
            }
          }
          .use { ec =>
            new XmlExtractor(Blocker.liftExecutionContext(ec))
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
