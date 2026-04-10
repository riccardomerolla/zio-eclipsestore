package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.config.NativeLocalSerde

object NativeLocalMemoryStress extends ZIOAppDefault:
  private enum OutputFormat:
    case Text
    case Json

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      val parsed = parseArgs(args)
      val output = if parsed.get("output").exists(_.equalsIgnoreCase("json")) then OutputFormat.Json else OutputFormat.Text
      val scenario =
        BenchScenarioConfig(
          serde = parsed
            .get("serde")
            .filter(_.equalsIgnoreCase("protobuf"))
            .fold[NativeLocalSerde](NativeLocalSerde.Json)(_ => NativeLocalSerde.Protobuf),
          elementCount = parsed.get("size").fold(10000)(_.toInt),
          nestedPayloadSize = parsed.get("nested-payload-size").fold(16)(_.toInt),
          concurrency = parsed.get("concurrency").fold(8)(_.toInt),
          checkpointCadence = CheckpointCadence.parse(parsed.getOrElse("checkpoint-cadence", "every-100")),
          payloadShape = BenchPayloadShape.parse(parsed.getOrElse("shape", "flat")),
          warmupSeconds = parsed.get("warmup-seconds").fold(2)(_.toInt),
          measurementSeconds = parsed.get("measurement-seconds").fold(5)(_.toInt),
          workload = StressWorkload.parse(parsed.getOrElse("workload", "mixed-80-20")),
          machine = parsed.get("machine"),
        )

      ZIO.scoped {
        ZIO.acquireRelease(
          ZIO.attempt(Files.createTempDirectory("native-local-memory-stress")).mapError(RuntimeException(_))
        )(path =>
          ZIO.attemptBlocking {
            if Files.exists(path) then
              Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
          }.ignore
        ).flatMap { baseDir =>
          NativeLocalStressRunner.run(scenario, baseDir).flatMap { report =>
            val body =
              output match
                case OutputFormat.Text => NativeLocalStressRunner.formatText(report)
                case OutputFormat.Json => NativeLocalStressRunner.formatJson(report)
            Console.printLine(body).orDie
          }
        }
      }.catchAll(error => ZIO.logError(error.toString))
    }

  private def parseArgs(args: Chunk[String]): Map[String, String] =
    args.foldLeft(Map.empty[String, String]) { (acc, arg) =>
      arg.split("=", 2).toList match
        case key :: value :: Nil if key.startsWith("--") => acc.updated(key.stripPrefix("--"), value)
        case _                                           => acc
    }
