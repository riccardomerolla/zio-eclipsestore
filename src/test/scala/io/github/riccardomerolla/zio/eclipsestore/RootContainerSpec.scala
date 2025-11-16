package io.github.riccardomerolla.zio.eclipsestore

import zio.test.*

import java.util.concurrent.ConcurrentHashMap

import io.github.riccardomerolla.zio.eclipsestore.domain.{ RootContainer, RootDescriptor }

object RootContainerSpec extends ZIOSpecDefault:

  private val simpleDescriptor =
    RootDescriptor(
      id = "counter",
      initializer = () => 0,
      migrate = (value: Int) => value + 1,
    )

  private val mapDescriptor =
    RootDescriptor.concurrentMap[String, String]("map-root")

  override def spec =
    suite("RootContainer")(
      test("initializes descriptor when missing") {
        val container = RootContainer.empty
        val result    = container.ensure(simpleDescriptor)
        assertTrue(result == 0)
      },
      test("migrates existing root when ensure is invoked again") {
        val container = RootContainer.empty
        val initial   = container.ensure(simpleDescriptor)
        val migrated  = container.ensure(simpleDescriptor)
        assertTrue(initial == 0, migrated == 1)
      },
      test("replaceWith copies entries from another container") {
        val source      = RootContainer.empty
        val destination = RootContainer.empty
        val map         = destination.ensure(mapDescriptor)
        map.put("should-be-overwritten", "nope")
        val newData     = new ConcurrentHashMap[String, String]()
        newData.put("fresh", "value")
        source.ensure(mapDescriptor).putAll(newData)
        destination.replaceWith(source)
        val refreshed   = destination.ensure(mapDescriptor)
        assertTrue(refreshed.size() == 1, refreshed.get("fresh") == "value")
      },
    )
