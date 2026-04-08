package io.github.riccardomerolla.zio.eclipsestore

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.util.concurrent.atomic.AtomicInteger

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.EclipseStoreError

object LazyLoadingSpec extends ZIOSpecDefault:

  final case class LazyRoot(
    customers: Lazy[List[String]],
    invoices: Lazy[List[Int]],
    audits: Lazy[List[String]],
  ) extends Serializable

  final case class LazyEnvelope(value: Lazy[Int]) extends Serializable

  private def roundTrip[A](value: A): Task[A] =
    ZIO.attempt {
      val bytes = ByteArrayOutputStream()
      val out   = ObjectOutputStream(bytes)
      out.writeObject(value)
      out.flush()
      out.close()

      val in = ObjectInputStream(ByteArrayInputStream(bytes.toByteArray))
      try in.readObject().asInstanceOf[A]
      finally in.close()
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Lazy loading primitives")(
      test("only explicitly loaded lazy fields are materialized") {
        for
          loaded <- Ref.make(List.empty[String])
          root    = LazyRoot(
                      customers = Lazy.fromZIO(loaded.update(_ :+ "customers").as(List("alice", "bob"))),
                      invoices = Lazy.fromZIO(loaded.update(_ :+ "invoices").as(List(1, 2, 3))),
                      audits = Lazy.fromZIO(loaded.update(_ :+ "audits").as(List("created"))),
                    )
          _      <- root.customers.load
          _      <- root.invoices.load
          seen   <- loaded.get
        yield assertTrue(seen == List("customers", "invoices"))
      },
      test("concurrent loads coalesce to a single underlying fetch") {
        for
          starts  <- Ref.make(0)
          gate    <- Promise.make[Nothing, Unit]
          lazyRef  = Lazy.fromZIO(
                       starts.updateAndGet(_ + 1) *> gate.await *> ZIO.succeed("loaded-once")
                     )
          fibers  <- ZIO.foreach(1 to 20)(_ => lazyRef.load.fork)
          _       <- gate.succeed(())
          results <- ZIO.foreach(fibers)(_.join)
          count   <- starts.get
        yield assertTrue(results.forall(_ == "loaded-once"), count == 1)
      },
      test("loading after the backing store has closed fails with a typed store-not-open error") {
        val program =
          ZIO.scoped {
            for
              open   <- Ref.make(true)
              _      <- ZIO.addFinalizer(open.set(false))
              lazyRef = Lazy.fromZIO(
                          open.get.flatMap {
                            case true  => ZIO.succeed(42)
                            case false => ZIO.fail(EclipseStoreError.StoreNotOpen("Scoped loader is closed", None))
                          }
                        )
            yield lazyRef
          }

        for
          lazyRef <- program
          result  <- lazyRef.load.either
        yield assertTrue(
          result == Left(EclipseStoreError.StoreNotOpen("Scoped loader is closed", None))
        )
      },
      test("serialized lazy fields return unloaded and load on demand after deserialization") {
        for
          value       <- ZIO.succeed(LazyEnvelope(Lazy.succeed(99)))
          serialized  <- roundTrip(value)
          beforeLoad  <- serialized.value.isLoaded
          loadedValue <- serialized.value.load
        yield assertTrue(!beforeLoad, loadedValue == 99)
      },
      test("memoized successful loads stay observationally equivalent to a single load") {
        for
          counter <- ZIO.succeed(AtomicInteger(0))
          lazyRef  = Lazy.fromZIO(ZIO.succeed(counter.incrementAndGet()))
          first   <- lazyRef.load
          second  <- lazyRef.load
        yield assertTrue(first == 1, second == 1, counter.get() == 1)
      },
    )
