package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain

import zio.Chunk
import zio.json.*
import zio.schema.Schema
import zio.schema.derived

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import scala.jdk.CollectionConverters.*

final case class BookId(value: UUID) derives JsonCodec, Schema

final case class Book(
    id: BookId,
    title: String,
    author: String,
    price: BigDecimal,
    tags: Chunk[String] = Chunk.empty,
  ) derives JsonCodec, Schema

final case class CreateBookRequest(
    title: String,
    author: String,
    price: BigDecimal,
    tags: Chunk[String] = Chunk.empty,
  ) derives JsonCodec, Schema

final case class UpdateBookRequest(
    title: Option[String],
    author: Option[String],
    price: Option[BigDecimal],
    tags: Option[Chunk[String]],
  ) derives JsonCodec, Schema

final case class BookstoreRoot(storage: ConcurrentHashMap[UUID, Book])

object BookstoreRoot:
  given Schema[BookstoreRoot] =
    Schema.chunk(Schema.tuple2(Schema[UUID], Schema[Book])).transform(
      pairs =>
        val map = new ConcurrentHashMap[UUID, Book]()
        pairs.foreach { case (key, value) => map.put(key, value) }
        BookstoreRoot(map),
      root => Chunk.fromIterable(root.storage.entrySet().asScala.map(entry => (entry.getKey, entry.getValue))),
    )

  val descriptor: RootDescriptor[BookstoreRoot] =
    RootDescriptor.fromSchema(
      id = "bookstore-root",
      initializer = () => BookstoreRoot(new ConcurrentHashMap[UUID, Book]()),
    )
