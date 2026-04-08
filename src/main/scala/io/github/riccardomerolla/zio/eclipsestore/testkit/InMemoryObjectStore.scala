package io.github.riccardomerolla.zio.eclipsestore.testkit

import zio.*

import io.github.riccardomerolla.zio.eclipsestore.domain.RootDescriptor
import io.github.riccardomerolla.zio.eclipsestore.service.{ EclipseStoreService, ObjectStore }

object InMemoryObjectStore:
  def layer[Root: Tag](descriptor: RootDescriptor[Root]): ULayer[ObjectStore[Root]] =
    EclipseStoreService.inMemory >>> ObjectStore.live(descriptor)

