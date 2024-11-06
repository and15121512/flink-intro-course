import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._

object Module3Task1App {

  final case class Click(userId: String, clickTime: Instant)

  val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)

  val clicks = List(
    Click("550e8400", eventTime(1000L)),
    Click("550e6200", eventTime(1000L)),
    Click("550e1207", eventTime(1000L)),
    Click("550e8400", eventTime(2000L)),
    Click("550e6200", eventTime(2000L)),
    Click("550e1207", eventTime(3000L)),
    Click("550e8400", eventTime(4000L)),
    Click("550e1207", eventTime(4000L)),
    Click("550e6200", eventTime(4000L)),
    Click("550e1207", eventTime(8000L)),
    Click("550e6200", eventTime(8000L)),
    Click("550e1207", eventTime(9000L)),
    Click("550e1207", eventTime(10000L)),
    Click("550e1207", eventTime(11000L)),
    Click("550e6200", eventTime(11000L)),
    Click("550e1207", eventTime(12000L)),
    Click("550e8400", eventTime(12000L)),
    Click("550e1207", eventTime(18000L)),
  ).asJava

  final val maxOutOfOrderness = java.time.Duration.ofMillis(1000L)
  final val sessionWindowGap = Time.seconds(3)

  def getClicksLog(clicks: lang.Iterable[Click], window: TimeWindow) = {
    s"Window [${window.getStart} - ${window.getEnd}] click count: ${clicks.asScala.size}"
  }

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  class countClicksFunction extends AllWindowFunction[Click, String, TimeWindow] {
    override def apply(window: TimeWindow, clicks: lang.Iterable[Click], out: Collector[String]): Unit = {
      out.collect(getClicksLog(clicks, window))
    }
  }

  def impl() = {
    val clicksCountLogStream = env
      .fromCollection(clicks)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(maxOutOfOrderness)
          .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
            override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
              element.clickTime.toEpochMilli
            }
          })
      )
      .windowAll(EventTimeSessionWindows.withGap(sessionWindowGap))
      .apply(new countClicksFunction)

    clicksCountLogStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
