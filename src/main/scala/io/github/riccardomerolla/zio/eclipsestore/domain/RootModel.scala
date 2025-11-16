package io.github.riccardomerolla.zio.eclipsestore.domain

import java.util.concurrent.ConcurrentHashMap

import org.eclipse.serializer.persistence.types.{ Persister, Storer }
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager
import scala.jdk.CollectionConverters.*

/** Describes how to initialize and migrate a typed EclipseStore root instance. */
final case class RootDescriptor[A](
    id: String,
    initializer: () => A,
    migrate: A => A = (a: A) => a,
    onLoad: A => Unit = (_: A) => (),
  ):
  def map[B](f: A => B)(g: B => A): RootDescriptor[B] =
    RootDescriptor(
      id = id,
      initializer = () => f(initializer()),
      migrate = b => f(migrate(g(b))),
      onLoad = b => onLoad(g(b)),
    )

object RootDescriptor:
  def concurrentMap[K, V](id: String): RootDescriptor[ConcurrentHashMap[K, V]] =
    RootDescriptor(
      id = id,
      initializer = () => new ConcurrentHashMap[K, V](),
      migrate = (map: ConcurrentHashMap[K, V]) => map,
    )

/** Container that keeps track of all configured roots and applies migrations. */
final class RootContainer private[domain] (
    private val instances: ConcurrentHashMap[String, AnyRef]
  ) extends Serializable:

  def ensure[A](descriptor: RootDescriptor[A]): A =
    val value =
      Option(instances.get(descriptor.id)).map(_.asInstanceOf[A]) match
        case Some(existing) =>
          val migrated = descriptor.migrate(existing)
          instances.put(descriptor.id, migrated.asInstanceOf[AnyRef])
          migrated
        case None           =>
          val created = descriptor.initializer()
          instances.put(descriptor.id, created.asInstanceOf[AnyRef])
          created
    descriptor.onLoad(value)
    value

  def get[A](descriptor: RootDescriptor[A]): Option[A] =
    Option(instances.get(descriptor.id)).map(_.asInstanceOf[A])

  def descriptors: Set[String] =
    instances.keySet().asScala.toSet

  def replaceWith(other: RootContainer): Unit =
    instances.clear()
    instances.putAll(other.instances)

  private[eclipsestore] def instanceState: ConcurrentHashMap[String, AnyRef] =
    instances

object RootContainer:
  def empty: RootContainer =
    new RootContainer(new ConcurrentHashMap[String, AnyRef]())

  def from(map: ConcurrentHashMap[String, AnyRef]): RootContainer =
    new RootContainer(map)

/** Provides contextual handles for custom operations executed against EclipseStore. */
final case class RootContext(
    container: RootContainer,
    storageManager: Option[EmbeddedStorageManager],
    storer: Option[Storer],
    persister: Option[Persister],
  )
