package module5

import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.Random

case class Config(
              stores: Map[String, Double],
              appIds: Map[String, Double],
              eventTypes: Map[String, Double],
              eventTimeFreqSeconds: java.time.Duration
            )

object Config {

  def getConfig(configFileName: String): Config = {
    val configContent = getConfigFileSafely(configFileName)
    parseJsonConfig(configContent)
  }

  private def getConfigReadErrorMessage(ex: Exception) = {
    s"Failed to read config file for data generation ``. " +
      s"Error: ${ex.getMessage}"
  }

  private def getConfigFileSafely(configFileName: String): String = {
    val source = Source
      .fromFile(configFileName)
    try {
      source.mkString
    } catch {
      case ex: Exception => throw new RuntimeException(
        getConfigReadErrorMessage(ex)
      )
    } finally {
      source.close()
    }
  }

  private def parseJsonConfig(configJsonString: String): Config = {
    try {

      val config = ujson.read(configJsonString)

      val storesMap =
        config("stores").arr
          .map(_.obj)
          .map(store =>
            (store("store").str, store("store").num)
          )
          .toMap

      val appIdsMap =
        config("appIds").arr
          .map(_.obj)
          .map(store =>
            (store("store").str, store("store").num)
          )
          .toMap

      val eventTypesMap =
        config("eventTypes").arr
          .map(_.obj)
          .map(store =>
            (store("store").str, store("store").num)
          )
          .toMap

      val eventTimeFreqSecondsVal = config("eventTimeFreqSeconds").num.toLong

      new Config(storesMap, appIdsMap, eventTypesMap, java.time.Duration.ofMillis(eventTimeFreqSecondsVal))
    }
    catch {
      case ex: Exception => throw new RuntimeException(getConfigReadErrorMessage(ex))
    }
  }

}

