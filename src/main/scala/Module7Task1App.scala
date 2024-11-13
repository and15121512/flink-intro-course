import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStream.Collector
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._

object Module7Task1App {

  final case class Click(userId: String, clickTime: Instant)
  final case class ClickCount(wStart: Long, wEnd: Long, clickCount: Int)

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)


  val clicks = List(
    Click("550e8400", eventTime(1000L)),
    Click("550e6200", eventTime(1000L)),
    Click("550e1207", eventTime(1000L)),
    Click("550e8400", eventTime(2000L)),
    Click("550e8400", eventTime(3000L)),
    Click("550e1207", eventTime(3000L)),
    Click("550e8400", eventTime(4000L)),
    Click("550e1207", eventTime(4000L)),
    Click("550e6200", eventTime(5000L)),
    Click("550e1207", eventTime(6000L)),
    Click("550e6200", eventTime(6000L)),
    Click("550e1207", eventTime(7000L)),
    Click("550e1207", eventTime(8000L)),
    Click("550e1207", eventTime(9000L)),
    Click("550e6200", eventTime(10000L)),
    Click("550e1207", eventTime(11000L)),
    Click("550e8400", eventTime(12000L)),
    Click("550e1207", eventTime(12000L)),
  ).asJava

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  class TotalClicksTrigger

  class TotalClicks extends ProcessAllWindowFunction[Click, ClickCount, TimeWindow] {

    override def process(
                          context: ProcessAllWindowFunction[Click, ClickCount, TimeWindow]#Context,
                          elements: lang.Iterable[Click],
                          out: util.Collector[ClickCount]
                        ): Unit = {
      out.collect(
        ClickCount(context.window().getStart, context.window().getEnd, elements.asScala.size)
      )
    }
  }

  def impl(): Unit = {

    val clicksStream = env.fromCollection(clicks)

    val slidingWindowStream: SingleOutputStreamOperator[ClickCount] = clicksStream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
            override def extractTimestamp(element: Click, recordTimestamp: Long): Long = {
              element.clickTime.toEpochMilli
            }
          }))
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .process(new TotalClicks)

    val countedClicksStream = slidingWindowStream
      .windowAll(EventTimeSessionWindows.withGap(Time.seconds(30)))
      .reduce(new ReduceFunction[ClickCount] {
        override def reduce(value1: ClickCount, value2: ClickCount): ClickCount = {
          if (value1.clickCount >= value2.clickCount) value1
          else value2
        }
      })

    countedClicksStream.print()

    env.execute()
  }

  def main(args: Array[String]) = {
    impl()
  }
}
