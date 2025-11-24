package io.github.riccardomerolla.zio.eclipsestore.lazyloading.model

import org.eclipse.serializer.reference.Lazy

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.{ ArrayList, Iterator as JIterator, List as JList, Map as JMap }
import scala.jdk.CollectionConverters.*

final case class Turnover(amount: Double, timestamp: Instant)

final class BusinessYear(private var turnovers: Lazy[JList[Turnover]] = Lazy.Reference(new ArrayList[Turnover]())):
  def add(turnover: Turnover): Unit =
    Lazy.get(turnovers).add(turnover)

  def stream: Iterator[Turnover] =
    val list = Lazy.get(turnovers)
    if list == null then Iterator.empty
    else
      val it: JIterator[Turnover] = list.iterator()
      it.asScala

  def touchedTimestamp: Long = turnovers.lastTouched()

  def clearIfStored(): Unit =
    if Lazy.isStored(turnovers) then Lazy.clear(turnovers) else ()

final class BusinessApp(val businessYears: JMap[Int, BusinessYear] = new ConcurrentHashMap[Int, BusinessYear]()):
  def ensureYear(year: Int): BusinessYear =
    businessYears.computeIfAbsent(year, _ => new BusinessYear())
