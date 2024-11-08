import module5.{Config, EventContent, EventContentGenerator, EventType}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.io.Source


package object module5generator {

  case class Event(
                    store: String,
                    appId: String,
                    eventType: EventType,
                    eventTime: java.time.Instant
                  )

  class EventGenerator extends RichSourceFunction[Event] {

    val configFileName = "/data-generator-config.json"
    val millisBtwEvents = 50L

    val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")

    var configOpt: Option[Config] = None

    @volatile private var isRunning = true

    private def runImpl(ctx: SourceFunction.SourceContext[Event]): Unit = {
      var nowTime = startTime
      while (isRunning) {
        val eventContent = EventContentGenerator.getRecord(configOpt match {
          case Some(config) => config
          case None => throw new RuntimeException("Error: Config is not initialized !!!")
        })
        //println(s"Input event: ${eventContent}")
        ctx.collect(
          Event(
            eventContent.store,
            eventContent.appId,
            eventContent.eventType,
            nowTime
          )
        )
        Thread.sleep(millisBtwEvents)
        nowTime = nowTime.plus(java.time.Duration.ofMillis(millisBtwEvents))
        //run(startId + batchSize, ctx)
      }
    }

    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = runImpl(ctx)

    override def cancel(): Unit = {

    }

    override def open(parameters: Configuration): Unit = {
      configOpt = Some(Config.getConfig(configFileName))
    }

    override def close(): Unit = {
      super.close()
    }
  }

}
