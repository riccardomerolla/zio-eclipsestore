package io.github.riccardomerolla.zio.eclipsestore.gigamap

import zio.*
import zio.Chunk
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.gigamap.config.*
import io.github.riccardomerolla.zio.eclipsestore.gigamap.domain.GigaMapQuery
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

final case class Account(id: Int, owner: String, city: String, status: String, balance: Int)

object GigaMapSpec extends ZIOSpecDefault:

  private val definition =
    GigaMapDefinition[Int, Account](
      name = "accounts",
      indexes = Chunk(
        GigaMapIndex("city", _.city),
        GigaMapIndex("status", _.status),
      ),
      autoPersist = false,
    )

  private val layer =
    EclipseStoreService.inMemory >>> GigaMap.make(definition)

  private def withMap[R, A](f: GigaMap[Int, Account] => ZIO[Any, R, A]) =
    ZIO.serviceWithZIO[GigaMap[Int, Account]](f)

  override def spec =
    suite("GigaMap")(
      test("puts and retrieves entries") {
        val account = Account(1, "Alice", "Berlin", "active", 1200)
        for
          _       <- withMap(_.put(account.id, account))
          fetched <- withMap(_.get(1))
        yield assertTrue(fetched.contains(account))
      },
      test("queries by index") {
        val alice   = Account(1, "Alice", "Berlin", "active", 1200)
        val bob     = Account(2, "Bob", "Berlin", "active", 800)
        val charlie = Account(3, "Charlie", "Paris", "inactive", 300)
        for
          _      <- withMap(_.put(alice.id, alice))
          _      <- withMap(_.put(bob.id, bob))
          _      <- withMap(_.put(charlie.id, charlie))
          berlin <- withMap(_.query(GigaMapQuery.ByIndex("city", "Berlin")))
        yield assertTrue(berlin.map(_.owner).toSet == Set("Alice", "Bob"))
      },
      test("filters by predicate") {
        val small = Account(4, "Small", "Rome", "active", 100)
        val big   = Account(5, "Big", "Rome", "active", 1000)
        for
          _    <- withMap(_.put(small.id, small))
          _    <- withMap(_.put(big.id, big))
          rich <- withMap(_.query(GigaMapQuery.Filter[Account](_.balance >= 500)))
        yield assertTrue(rich.map(_.owner).toSet == Set("Big"))
      },
      test("counts entries") {
        for count <- withMap(_.query(GigaMapQuery.Count[Account]()))
        yield assertTrue(count >= 0)
      },
      test("removals clean index state") {
        val entry = Account(6, "Temp", "Madrid", "active", 200)
        for
          _       <- withMap(_.put(entry.id, entry))
          _       <- withMap(_.remove(entry.id))
          results <- withMap(_.query(GigaMapQuery.ByIndex("city", "Madrid")))
        yield assertTrue(results.isEmpty)
      },
    ).provideLayer(layer)
