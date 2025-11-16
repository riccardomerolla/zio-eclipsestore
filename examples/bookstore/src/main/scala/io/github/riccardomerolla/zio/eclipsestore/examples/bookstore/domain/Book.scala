package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain

import zio.Chunk
import zio.json.*

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor

final case class BookId(value: UUID) derives JsonCodec

final case class Book(
    id: BookId,
    title: String,
    author: String,
    price: BigDecimal,
    tags: Chunk[String] = Chunk.empty,
  ) derives JsonCodec

final case class CreateBookRequest(
    title: String,
    author: String,
    price: BigDecimal,
    tags: Chunk[String] = Chunk.empty,
  ) derives JsonCodec

final case class UpdateBookRequest(
    title: Option[String],
    author: Option[String],
    price: Option[BigDecimal],
    tags: Option[Chunk[String]],
  ) derives JsonCodec

final case class BookstoreRoot(storage: ConcurrentHashMap[UUID, Book])

object BookstoreRoot:
  val descriptor: RootDescriptor[BookstoreRoot] =
    RootDescriptor(
      id = "bookstore-root",
      initializer = () => BookstoreRoot(new ConcurrentHashMap[UUID, Book]()),
    )
