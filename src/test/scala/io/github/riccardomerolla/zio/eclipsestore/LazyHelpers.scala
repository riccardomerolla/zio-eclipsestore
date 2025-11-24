package io.github.riccardomerolla.zio.eclipsestore

import org.eclipse.serializer.reference.{ Lazy, LazyReferenceManager }

object LazyHelpers:
  def reference[T](value: T): Lazy[T] =
    Lazy.Reference(value)

  def newManager(): LazyReferenceManager =
    LazyReferenceManager.New()

  def isLoaded(ref: Lazy[?]): Boolean =
    Lazy.isLoaded(ref)
