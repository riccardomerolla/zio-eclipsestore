package io.github.riccardomerolla.zio.eclipsestore

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.domain.{ Query, RootDescriptor }
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, LifecycleCommand, LifecycleStatus }
import scala.collection.mutable.ListBuffer

object EclipseStoreServiceSpec extends ZIOSpecDefault:

  override def spec =
    suite("EclipseStoreService")(
      test("stores and retrieves a single value") {
        for
          _      <- EclipseStoreService.put("key1", "value1")
          result <- EclipseStoreService.get[String, String]("key1")
        yield assertTrue(result.contains("value1"))
      },
      test("returns None for non-existent key") {
        for result <- EclipseStoreService.get[String, String]("nonexistent")
        yield assertTrue(result.isEmpty)
      },
      test("stores and retrieves multiple values") {
        for
          _ <- EclipseStoreService.put("user:1", "Alice")
          _ <- EclipseStoreService.put("user:2", "Bob")
          _ <- EclipseStoreService.put("user:3", "Charlie")

          user1 <- EclipseStoreService.get[String, String]("user:1")
          user2 <- EclipseStoreService.get[String, String]("user:2")
          user3 <- EclipseStoreService.get[String, String]("user:3")
        yield assertTrue(
          user1.contains("Alice") &&
          user2.contains("Bob") &&
          user3.contains("Charlie")
        )
      },
      test("deletes a value") {
        for
          _            <- EclipseStoreService.put("toDelete", "value")
          beforeDelete <- EclipseStoreService.get[String, String]("toDelete")
          _            <- EclipseStoreService.delete("toDelete")
          afterDelete  <- EclipseStoreService.get[String, String]("toDelete")
        yield assertTrue(
          beforeDelete.contains("value") &&
          afterDelete.isEmpty
        )
      },
      test("retrieves all values") {
        for
          _ <- EclipseStoreService.put("a", 1)
          _ <- EclipseStoreService.put("b", 2)
          _ <- EclipseStoreService.put("c", 3)

          allValues <- EclipseStoreService.getAll[Int]
        yield assertTrue(
          allValues.toSet == Set(1, 2, 3)
        )
      },
      test("executes multiple queries in batch") {
        for
          _ <- EclipseStoreService.put("batch:1", "First")
          _ <- EclipseStoreService.put("batch:2", "Second")
          _ <- EclipseStoreService.put("batch:3", "Third")

          queries  = List(
                       Query.Get[String, String]("batch:1"),
                       Query.Get[String, String]("batch:2"),
                       Query.Get[String, String]("batch:3"),
                     )
          results <- EclipseStoreService.executeMany(queries)
        yield assertTrue(
          results == List(Some("First"), Some("Second"), Some("Third"))
        )
      },
      test("handles mixed batchable and non-batchable queries") {
        for
          _ <- EclipseStoreService.put("item:1", "A")
          _ <- EclipseStoreService.put("item:2", "B")

          queries  = List(
                       Query.Get[String, String]("item:1"), // batchable
                       Query.GetAllValues[String](),        // non-batchable
                       Query.Get[String, String]("item:2"), // batchable
                     )
          results <- EclipseStoreService.executeMany(queries)

          getValue1    = results.head.asInstanceOf[Option[String]]
          getAllValues = results(1).asInstanceOf[List[String]]
          getValue2    = results(2).asInstanceOf[Option[String]]
        yield assertTrue(
          getValue1.contains("A") &&
          getAllValues.toSet.contains("A") &&
          getAllValues.toSet.contains("B") &&
          getValue2.contains("B")
        )
      },
      test("overwrites existing value") {
        for
          _      <- EclipseStoreService.put("key", "original")
          before <- EclipseStoreService.get[String, String]("key")
          _      <- EclipseStoreService.put("key", "updated")
          after  <- EclipseStoreService.get[String, String]("key")
        yield assertTrue(
          before.contains("original") &&
          after.contains("updated")
        )
      },
      test("stores batch entries and streams them") {
        for
          _        <- EclipseStoreService.putAll(List("stream:1" -> "one", "stream:2" -> "two"))
          streamed <- EclipseStoreService.streamValues[String].runCollect
        yield assertTrue(streamed.toSet.contains("one") && streamed.toSet.contains("two"))
      },
      test("exposes typed roots") {
        val descriptor = RootDescriptor(
          id = "users",
          initializer = () => ListBuffer.empty[String],
        )
        for
          root     <- EclipseStoreService.root(descriptor)
          _        <- ZIO.succeed(root.addOne("alice"))
          hydrated <- EclipseStoreService.root(descriptor)
        yield assertTrue(hydrated.contains("alice"))
      },
      test("executes custom queries against root context") {
        val countQuery = Query.Custom[Int](
          operation = "count",
          run = ctx => ctx.container.ensure(RootDescriptor.concurrentMap[Any, Any]("kv-root")).size(),
        )
        for
          _     <- EclipseStoreService.put("ctx:1", 1)
          count <- EclipseStoreService.execute(countQuery)
        yield assertTrue(count >= 1)
      },
      test("lifecycle maintenance updates status") {
        for
          _       <- EclipseStoreService.maintenance(LifecycleCommand.Checkpoint)
          running <- EclipseStoreService.status
          _       <- EclipseStoreService.maintenance(LifecycleCommand.Shutdown)
          stopped <- EclipseStoreService.status
        yield assertTrue(
          running match
            case LifecycleStatus.Running(_) => true
            case _                          => false,
          stopped == LifecycleStatus.Stopped,
        )
      },
    ).provide(EclipseStoreService.inMemory)
