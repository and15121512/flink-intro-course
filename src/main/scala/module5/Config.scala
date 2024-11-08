package module5

import java.io.{File, InputStream}
import java.nio.file.Paths
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

    val inputStream = getClass.getResourceAsStream(configFileName)
    if (inputStream != null) {
      val source = Source.fromInputStream(inputStream)
      val content = source.mkString
      source.close()
      content
    }
    else {
      throw new RuntimeException(s"Can't find config file `${configFileName}`")
    }
  }

  private def parseJsonConfig(configJsonString: String): Config = {
    try {

      val config = ujson.read(configJsonString)

      val storesMap =
        config("stores").arr
          .map(_.obj)
          .map(store =>
            (store("name").str, store("probability").num)
          )
          .toMap

      val appIdsMap =
        config("appIds").arr
          .map(_.obj)
          .map(store =>
            (store("name").str, store("probability").num)
          )
          .toMap

      val eventTypesMap =
        config("eventTypes").arr
          .map(_.obj)
          .map(store =>
            (store("name").str, store("probability").num)
          )
          .toMap

      val eventTimeFreqSecondsVal = config("eventTimeFreqMillis").num.toLong

      /*println(storesMap)
      println(appIdsMap)
      println(eventTypesMap)
      println(java.time.Duration.ofMillis(eventTimeFreqSecondsVal))*/

      new Config(storesMap, appIdsMap, eventTypesMap, java.time.Duration.ofMillis(eventTimeFreqSecondsVal))
    }
    catch {
      case ex: Exception => throw new RuntimeException(getConfigReadErrorMessage(ex))
    }
  }

}

