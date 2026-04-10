package io.github.riccardomerolla.zio.eclipsestore.bench

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ DeriveSchema, Schema }

import io.github.riccardomerolla.zio.eclipsestore.config.{ NativeLocalEventingConfig, NativeLocalSerde }
import io.github.riccardomerolla.zio.eclipsestore.service.*

object NativeLocalEventingStress extends ZIOAppDefault:

  enum CounterCommand:
    case Add(delta: Int)

  object CounterCommand:
    given Schema[CounterCommand] = DeriveSchema.gen[CounterCommand]

  enum CounterEvent:
    case Added(delta: Int)

  object CounterEvent:
    given Schema[CounterEvent] = DeriveSchema.gen[CounterEvent]

  final case class CounterState(total: Int)

  object CounterState:
    given Schema[CounterState] = DeriveSchema.gen[CounterState]

  enum CounterDomainError:
    case InvalidDelta

  private object CounterBehavior extends EventSourcedBehavior[CounterState, CounterCommand, CounterEvent, CounterDomainError]:
    override val zero: CounterState =
      CounterState(0)

    override def decide(
      state: CounterState,
      command: CounterCommand,
    ): IO[CounterDomainError, Chunk[CounterEvent]] =
      command match
        case CounterCommand.Add(delta) if delta <= 0 => ZIO.fail(CounterDomainError.InvalidDelta)
        case CounterCommand.Add(delta)               => ZIO.succeed(Chunk.single(CounterEvent.Added(delta)))

    override def evolve(
      state: CounterState,
      event: CounterEvent,
    ): CounterState =
      event match
        case CounterEvent.Added(delta) => state.copy(total = state.total + delta)

  private final case class Report(
    serde: NativeLocalSerde,
    concurrency: Int,
    warmupSeconds: Int,
    measurementSeconds: Int,
    totalCommands: Long,
    operationsPerSecond: Double,
    loadLatency: LatencySummary,
    appendLatency: LatencySummary,
    restartMillis: Long,
    streamCount: Int,
    totalEvents: Long,
  )

  private enum OutputFormat:
    case Text
    case Json

  override def run: URIO[ZIOAppArgs & Scope, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      val parsed = parseArgs(args)
      val serde =
        if parsed.get("serde").exists(_.equalsIgnoreCase("protobuf")) then NativeLocalSerde.Protobuf else NativeLocalSerde.Json
      val concurrency        = parsed.get("concurrency").fold(8)(_.toInt)
      val warmupSeconds      = parsed.get("warmup-seconds").fold(2)(_.toInt)
      val measurementSeconds = parsed.get("measurement-seconds").fold(5)(_.toInt)
      val output             = if parsed.get("output").exists(_.equalsIgnoreCase("json")) then OutputFormat.Json else OutputFormat.Text

      ZIO.scoped {
        ZIO.acquireRelease(
          ZIO.attempt(Files.createTempDirectory("native-local-eventing-stress")).mapError(RuntimeException(_))
        )(path =>
          ZIO.attemptBlocking {
            if Files.exists(path) then
              Files.walk(path).iterator().asScala.toList.sortBy(_.toString).reverse.foreach(Files.deleteIfExists)
          }.ignore
        ).flatMap { dir =>
          runScenario(NativeLocalEventingConfig(dir.resolve("eventing"), serde), concurrency, warmupSeconds, measurementSeconds)
            .flatMap { report =>
              val rendered =
                output match
                  case OutputFormat.Text => formatText(report)
                  case OutputFormat.Json => formatJson(report)
              Console.printLine(rendered).orDie
            }
        }
      }.catchAll(error => ZIO.logError(error.toString))
    }

  private def runScenario(
    config: NativeLocalEventingConfig,
    concurrency: Int,
    warmupSeconds: Int,
    measurementSeconds: Int,
  ): ZIO[Scope, RuntimeException, Report] =
    type RuntimeService = EventSourcedRuntime[CounterState, CounterCommand, CounterEvent, CounterDomainError]

    val layer: ULayer[RuntimeService & EventStore[CounterEvent]] =
      ZLayer.make[RuntimeService & EventStore[CounterEvent]](
        NativeLocalEventStore.live[CounterEvent](config),
        NativeLocalSnapshotStore.live[CounterState](config),
        EventSourcedRuntimeLive.layer(
          CounterBehavior,
          SnapshotPolicy.everyNEvents[CounterState, CounterEvent](100),
        ),
      )

    val streamIds = Chunk.fromIterable(0 until math.max(1, concurrency)).map(index => StreamId(s"counter-$index"))

    ZIO.scoped {
      for
        env            <- layer.build
        warmupDeadline <- Clock.instant.map(_.plusSeconds(warmupSeconds.toLong))
        _              <- driveWorkers(streamIds, warmupDeadline, None).provideEnvironment(env)
        latencyRef     <- Ref.make((Chunk.empty[Long], Chunk.empty[Long], 0L))
        measurementEnd <- Clock.instant.map(_.plusSeconds(measurementSeconds.toLong))
        startedAt      <- Clock.nanoTime
        fibers         <- ZIO.foreach(streamIds) { streamId =>
                            driveWorkers(streamIds, measurementEnd, Some(latencyRef), streamId).provideEnvironment(env).forkScoped
                          }
        _              <- ZIO.foreachDiscard(fibers)(_.join)
        finishedAt     <- Clock.nanoTime
        restartStart   <- Clock.nanoTime
        freshEnv       <- layer.fresh.build
        counts         <- ZIO.foreach(streamIds)(id =>
                            EventSourcedRuntime
                              .load[CounterState, CounterCommand, CounterEvent, CounterDomainError](id)
                              .map(_.state.total)
                              .provideEnvironment(freshEnv)
                              .mapError(error => RuntimeException(error.toString))
                          )
        restartEnd     <- Clock.nanoTime
        metrics        <- latencyRef.get
        totalEvents    <- ZIO.foreach(streamIds)(id =>
                            EventStore.readStream[CounterEvent](id).provideEnvironment(env).map(_.size.toLong).mapError(error =>
                              RuntimeException(error.toString)
                            )
                          ).map(_.sum)
        elapsedMillis   = math.max(1L, (finishedAt - startedAt) / 1_000_000L)
        _              <- ZIO.fail(RuntimeException("Eventing restart verification failed")).when(counts.exists(_ <= 0))
      yield Report(
        serde = config.serde,
        concurrency = concurrency,
        warmupSeconds = warmupSeconds,
        measurementSeconds = measurementSeconds,
        totalCommands = metrics._3,
        operationsPerSecond = metrics._3.toDouble / (elapsedMillis.toDouble / 1000.0),
        loadLatency = LatencySummary.fromNanos(metrics._1),
        appendLatency = LatencySummary.fromNanos(metrics._2),
        restartMillis = (restartEnd - restartStart) / 1_000_000L,
        streamCount = streamIds.size,
        totalEvents = totalEvents,
      )
    }

  private def driveWorkers(
    streamIds: Chunk[StreamId],
    deadline: java.time.Instant,
    metrics: Option[Ref[(Chunk[Long], Chunk[Long], Long)]],
    preferredStream: StreamId = StreamId("counter-0"),
  ): ZIO[EventSourcedRuntime[CounterState, CounterCommand, CounterEvent, CounterDomainError], RuntimeException, Unit] =
    Clock.instant.flatMap { now =>
      if now.isAfter(deadline) then ZIO.unit
      else
        for
          doRead    <- Random.nextBoolean
          streamId  <- if streamIds.nonEmpty then Random.nextIntBetween(0, streamIds.size).map(streamIds(_)) else ZIO.succeed(preferredStream)
          start     <- Clock.nanoTime
          _         <- (
                         if doRead then
                           EventSourcedRuntime.load[CounterState, CounterCommand, CounterEvent, CounterDomainError](streamId).unit
                         else
                           EventSourcedRuntime
                             .handle[CounterState, CounterCommand, CounterEvent, CounterDomainError](
                               streamId,
                               ExpectedVersion.Any,
                               CounterCommand.Add(1),
                             )
                             .unit
                       ).mapError(error => RuntimeException(error.toString))
          finish    <- Clock.nanoTime
          _         <- metrics match
                         case None      => ZIO.unit
                         case Some(ref) =>
                           ref.update { case (loads, appends, total) =>
                             if doRead then (loads :+ (finish - start), appends, total + 1L)
                             else (loads, appends :+ (finish - start), total + 1L)
                           }
          _         <- driveWorkers(streamIds, deadline, metrics, preferredStream)
        yield ()
    }

  private def formatText(report: Report): String =
    s"""NativeLocal Eventing Stress Report
       |serde=${report.serde}
       |concurrency=${report.concurrency}
       |warmupSeconds=${report.warmupSeconds}
       |measurementSeconds=${report.measurementSeconds}
       |streams=${report.streamCount}
       |events=${report.totalEvents}
       |commands=${report.totalCommands}
       |opsPerSecond=${"%.2f".format(report.operationsPerSecond)}
       |load.p50=${"%.3f".format(report.loadLatency.p50Millis)}ms
       |load.p95=${"%.3f".format(report.loadLatency.p95Millis)}ms
       |append.p50=${"%.3f".format(report.appendLatency.p50Millis)}ms
       |append.p95=${"%.3f".format(report.appendLatency.p95Millis)}ms
       |restartMillis=${report.restartMillis}
       |""".stripMargin

  private def formatJson(report: Report): String =
    s"""{
       |  "serde": "${report.serde.toString}",
       |  "concurrency": ${report.concurrency},
       |  "warmupSeconds": ${report.warmupSeconds},
       |  "measurementSeconds": ${report.measurementSeconds},
       |  "streams": ${report.streamCount},
       |  "events": ${report.totalEvents},
       |  "commands": ${report.totalCommands},
       |  "operationsPerSecond": ${"%.4f".format(report.operationsPerSecond)},
       |  "restartMillis": ${report.restartMillis},
       |  "latencyMillis": {
       |    "load": {"count": ${report.loadLatency.count}, "p50": ${"%.4f".format(report.loadLatency.p50Millis)}, "p95": ${"%.4f".format(report.loadLatency.p95Millis)}, "p99": ${"%.4f".format(report.loadLatency.p99Millis)}},
       |    "append": {"count": ${report.appendLatency.count}, "p50": ${"%.4f".format(report.appendLatency.p50Millis)}, "p95": ${"%.4f".format(report.appendLatency.p95Millis)}, "p99": ${"%.4f".format(report.appendLatency.p99Millis)}}
       |  }
       |}""".stripMargin

  private def parseArgs(args: Chunk[String]): Map[String, String] =
    args.foldLeft(Map.empty[String, String]) { (acc, arg) =>
      arg.split("=", 2).toList match
        case key :: value :: Nil if key.startsWith("--") => acc.updated(key.stripPrefix("--"), value)
        case _                                           => acc
    }
