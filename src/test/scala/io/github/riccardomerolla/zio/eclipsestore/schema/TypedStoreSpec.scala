package io.github.riccardomerolla.zio.eclipsestore.schema

import java.time.Instant

import zio.*
import zio.schema.{ Schema, derived }
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService

object TypedStoreSpec extends ZIOSpecDefault:
  final case class User(id: String, name: String, createdAt: Instant) derives Schema
  final case class UsersRoot(values: List[User]) derives Schema

  private val layer = EclipseStoreService.inMemory >>> TypedStore.live

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TypedStore")(
      test("store / fetch / remove are schema-aware and typed") {
        val user = User("u1", "alice", Instant.ofEpochMilli(1_700_000_000_000L))
        for
          _      <- TypedStore.store("user:u1", user)
          before <- TypedStore.fetch[String, User]("user:u1")
          _      <- TypedStore.remove("user:u1")
          after  <- TypedStore.fetch[String, User]("user:u1")
        yield assertTrue(before.contains(user), after.isEmpty)
      },
      test("fetch returns None for missing key") {
        TypedStore.fetch[String, User]("missing-key").map(out => assertTrue(out.isEmpty))
      },
      test("fetchAll / streamAll return typed collections") {
        val a = User("u1", "alice", Instant.ofEpochMilli(1L))
        val b = User("u2", "bob", Instant.ofEpochMilli(2L))
        for
          _        <- TypedStore.store("a", a)
          _        <- TypedStore.store("b", b)
          all      <- TypedStore.fetchAll[User]
          streamed <- TypedStore.streamAll[User].runCollect
        yield assertTrue(all.toSet == Set(a, b), streamed.toSet == Set(a, b))
      },
      test("typedRoot and storePersist delegate to low-level service safely") {
        val descriptor = RootDescriptor.fromSchema[UsersRoot]("users-root", () => UsersRoot(Nil))
        val user       = User("u3", "chris", Instant.ofEpochMilli(3L))
        for
          root <- TypedStore.typedRoot(descriptor)
          _    <- TypedStore.storePersist(user)
        yield assertTrue(root.values.isEmpty)
      },
    ).provideLayer(layer)
