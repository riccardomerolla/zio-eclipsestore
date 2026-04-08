package io.github.riccardomerolla.zio.eclipsestore.testkit

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Gen

object PersistenceGenerators:
  final case class MessageEvent(id: String, body: String, version: Int)
  given Schema[MessageEvent] = DeriveSchema.gen[MessageEvent]

  val messageEvent: Gen[Any, MessageEvent] =
    for
      id      <- Gen.alphaNumericStringBounded(1, 12)
      body    <- Gen.alphaNumericStringBounded(1, 32)
      version <- Gen.int(1, 100)
    yield MessageEvent(id, body, version)

  val stringMap: Gen[Any, ConcurrentHashMap[String, String]] =
    Gen.listOfBounded(1, 8)(
      Gen.alphaNumericStringBounded(1, 12).zip(Gen.alphaNumericStringBounded(1, 12))
    ).map { entries =>
      val map = new ConcurrentHashMap[String, String]()
      entries.foreach { case (k, v) => map.put(k, v) }
      map
    }
