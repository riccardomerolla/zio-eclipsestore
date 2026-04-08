package io.github.riccardomerolla.zio.eclipsestore.error

/** Marker trait for typed persistence-domain failures exposed by the public API.
  *
  * Concrete subsystems may refine this contract with narrower ADTs, but public storage-facing error channels should
  * remain inside this hierarchy instead of leaking raw `Throwable` values.
  */
trait PersistenceError extends Product with Serializable
