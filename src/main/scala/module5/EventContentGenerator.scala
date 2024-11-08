package module5

import scala.util.Random

object EventContentGenerator {

  def getRecord(config: Config): EventContent = {
    EventContent(
      getEntity(config.stores),
      getEntity(config.appIds),
      getEventTypeSafely(getEntity(config.eventTypes))
    )
  }

  def getEventTypeSafely(name: String) = {
    EventType.getByName(name) match {
      case Some(eventType) => eventType
      case None => throw new RuntimeException(s"Invalid EventType found: `${name}`")
    }
  }

  def getEntity[E](entityProbabilities: Map[E, Double]): E = {
    val p = Random.nextDouble
    val it = entityProbabilities.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p) {
        return item
      } // return so that we don't have to search through the whole distribution
    }
    throw new RuntimeException("Error: Reached unreachable code zone in `getEntity`")
  }

}
