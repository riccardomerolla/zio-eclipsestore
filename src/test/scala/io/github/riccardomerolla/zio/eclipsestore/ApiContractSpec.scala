package io.github.riccardomerolla.zio.eclipsestore

import java.lang.reflect.{ Method, Modifier, ParameterizedType, Type, TypeVariable }

import zio.*
import zio.stream.ZStream
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.{
  EclipseStoreError,
  EventStoreError,
  OutboxError,
  PersistenceError,
  SnapshotStoreError,
}
import io.github.riccardomerolla.zio.eclipsestore.schema.TypedStore
import io.github.riccardomerolla.zio.eclipsestore.service.*

object ApiContractSpec extends ZIOSpecDefault:

  private val publicApiTypes = List(
    classOf[EclipseStoreService],
    classOf[EventPublisher],
    classOf[EventStore[?]],
    classOf[SnapshotStore[?]],
    classOf[OutboxRelay[?]],
    classOf[EventSourcedRuntime[?, ?, ?, ?]],
    classOf[LocalRepo[?, ?]],
    classOf[NativeLocalSTM[?]],
    classOf[TypedStore],
  )

  private val zioReturnType     = classOf[ZIO[Any, Any, Any]]
  private val zstreamReturnType = classOf[ZStream[Any, Any, Any]]

  private def publicDeclaredMethods(cls: Class[?]): List[Method] =
    cls.getDeclaredMethods.toList.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
      Modifier.isAbstract(m.getModifiers) &&
      !Modifier.isStatic(m.getModifiers) &&
      !m.isSynthetic
    }

  private def exposedBoundaryTypes(method: Method): List[Class[?]] =
    def topLevelClass(tpe: Type): List[Class[?]] =
      tpe match
        case cls: Class[?]                    => List(cls)
        case parameterized: ParameterizedType =>
          parameterized.getRawType match
            case cls: Class[?] => List(cls)
            case _             => Nil
        case _: TypeVariable[?]               => Nil
        case _                                => Nil

    method.getGenericParameterTypes.toList.flatMap(topLevelClass) ++ topLevelClass(method.getGenericReturnType)

  private def formatMethod(method: Method): String =
    s"${method.getDeclaringClass.getSimpleName}.${method.getName}"

  override def spec: Spec[TestEnvironment, Any] =
    suite("Scala-native API contract")(
      test("public service methods return effect or stream descriptions") {
        val invalidMethods =
          publicApiTypes.flatMap(publicDeclaredMethods).filterNot { method =>
            val returnType = method.getReturnType
            zioReturnType.isAssignableFrom(returnType) ||
            zstreamReturnType.isAssignableFrom(returnType)
          }

        assertTrue(invalidMethods.isEmpty)
      },
      test("public service methods do not expose java.util collections or raw Object") {
        val violations =
          publicApiTypes.flatMap(publicDeclaredMethods).flatMap { method =>
            val referencedTypes = exposedBoundaryTypes(method)
            val badTypes        = referencedTypes.filter(t => t == classOf[Object] || t.getName.startsWith("java.util."))
            badTypes.map(t => s"${formatMethod(method)} exposes ${t.getName}")
          }

        assertTrue(violations.isEmpty)
      },
      test("public storage error ADTs share the PersistenceError contract") {
        assertTrue(
          classOf[PersistenceError].isAssignableFrom(classOf[EclipseStoreError]),
          classOf[PersistenceError].isAssignableFrom(classOf[EventStoreError]),
          classOf[PersistenceError].isAssignableFrom(classOf[SnapshotStoreError]),
          classOf[PersistenceError].isAssignableFrom(classOf[OutboxError]),
        )
      },
    )
