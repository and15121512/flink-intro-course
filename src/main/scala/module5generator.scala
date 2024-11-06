import module5.Config
import module5.Config.EventContent
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.concurrent.duration.Duration
import scala.io.Source


package object module5generator {

  val configFileName = "data-generator-config.json"

  case class Event(
                    store: String,
                    appId: String,
                    eventType: String,
                    eventTime: java.time.Instant
                  )

  class EventGenerator(
                      batchSize: Int,
                      baseTime: java.time.Instant

                      ) extends RichSourceFunction[Event] {

    val configFileName = "data-generator-config.json"

    var config: Option[Config] = None

    @volatile private var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
      while (isRunning) {
        generateClick(startId).foreach(ctx.collect)
        Thread.sleep(batchSize * millisBtwEvents)
        run(startId + batchSize, ctx)
      }
    }

    override def cancel(): Unit = {

    }

    override def open(parameters: Configuration): Unit = {
      config = Some(Config.getConfig(configFileName))
    }

    override def close(): Unit = {
      super.close()
    }
  }

}
