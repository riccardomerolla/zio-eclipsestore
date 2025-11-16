package io.github.riccardomerolla.zio.eclipsestore.examples.bookstore

import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.domain.*
import io.github.riccardomerolla.zio.eclipsestore.examples.bookstore.service.BookRepository
import io.github.riccardomerolla.zio.eclipsestore.service.EclipseStoreService
import zio.*
import zio.test.*

object BookRepositorySpec extends ZIOSpecDefault:

  private val testLayer: ULayer[BookRepository] =
    EclipseStoreService.inMemory >>> BookRepository.live

  override def spec =
    suite("BookRepository")(
      test("creates and retrieves books") {
        for
          created <- BookRepository.create(CreateBookRequest("Clean Code", "Uncle Bob", BigDecimal(30)))
          fetched <- BookRepository.get(created.id)
        yield assertTrue(
          fetched.contains(created)
        )
      },
      test("lists books in insertion order") {
        for
          _ <- BookRepository.create(CreateBookRequest("ZIO", "John", BigDecimal(10)))
          _ <- BookRepository.create(CreateBookRequest("EclipseStore", "Alice", BigDecimal(20)))
          list <- BookRepository.list
        yield assertTrue(list.length == 2)
      },
      test("updates and deletes books") {
        for
          created <- BookRepository.create(CreateBookRequest("Old Title", "Author", BigDecimal(15)))
          updated <- BookRepository.update(created.id, UpdateBookRequest(title = Some("New Title"), author = None, price = None, tags = None))
          _ <- BookRepository.delete(created.id)
          afterDelete <- BookRepository.get(created.id)
        yield assertTrue(updated.title == "New Title", afterDelete.isEmpty)
      }
    ).provideLayer(testLayer)
