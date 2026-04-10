package io.github.riccardomerolla.zio.eclipsestore.gigamap

import java.lang.reflect.Method
import java.lang.reflect.Modifier

import zio.*
import zio.test.*

import io.github.riccardomerolla.zio.eclipsestore.error.PersistenceError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.error.GigaMapError
import io.github.riccardomerolla.zio.eclipsestore.gigamap.service.GigaMap

object GigaMapApiContractSpec extends ZIOSpecDefault:

  private val zioReturnType = classOf[ZIO[Any, Any, Any]]

  private def publicDeclaredMethods(cls: Class[?]): List[Method] =
    cls.getDeclaredMethods.toList.filter(m => Modifier.isPublic(m.getModifiers) && !m.isSynthetic)

  override def spec: Spec[TestEnvironment, Any] =
    suite("GigaMap API contract")(
      test("public methods return ZIO descriptions") {
        val invalidMethods =
          publicDeclaredMethods(classOf[GigaMap[Any, Any]].asInstanceOf[Class[?]]).filterNot { method =>
            zioReturnType.isAssignableFrom(method.getReturnType)
          }

        assertTrue(invalidMethods.isEmpty)
      },
      test("public error ADT shares the PersistenceError contract") {
        assertTrue(classOf[PersistenceError].isAssignableFrom(classOf[GigaMapError]))
      },
    )
