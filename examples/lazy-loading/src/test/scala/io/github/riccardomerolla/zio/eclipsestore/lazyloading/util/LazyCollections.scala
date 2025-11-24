package io.github.riccardomerolla.zio.eclipsestore.lazyloading.util

import org.eclipse.serializer.reference.Lazy

import java.util.concurrent.ConcurrentHashMap
import java.util.{ ArrayList, List as JList, Map as JMap }

object LazyCollections:
  def lazyList[A](initial: => JList[A] = new ArrayList[A]()): Lazy[JList[A]] =
    Lazy.Reference(initial)

  def lazyMap[K, V](initial: => JMap[K, V] = new ConcurrentHashMap[K, V]()): Lazy[JMap[K, V]] =
    Lazy.Reference(initial)

  def touched(ref: Lazy[?]): Long =
    if ref == null then 0L else ref.lastTouched()

  def clearIfStored(ref: Lazy[?]): Boolean =
    if ref != null && Lazy.isStored(ref) then
      Lazy.clear(ref)
      true
    else false
