import module4generator._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.time.Instant

object Module4Task1App {

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")

  val clicksStream: DataStreamSource[Click] = env.addSource(new ClickGenerator(1, startTime, 500))

  val maxOutOfOrderness = java.time.Duration.ofMillis(1000L)

  val minClicksDiffToOutput = 5

  def needToOutputClick(lastClickCount: Long, currentClickCount: Long): Boolean = {
    ((currentClickCount == 0) || ((currentClickCount - lastClickCount) > minClicksDiffToOutput))
  }

  case class ClickWithLastClicksCount(click: Click, lastClicksCount: Long)

  def impl(): Unit = {

    val clicksByUserStream = clicksStream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(maxOutOfOrderness)
          .withTimestampAssigner(new SerializableTimestampAssigner[Click] {
            override def extractTimestamp(click: Click, recordTimestamp: Long): Long = {
              click.time.toEpochMilli
            }
          })
      )
      .keyBy((click: Click) => click.userId)
      .process(new KeyedProcessFunction[String, Click, ClickWithLastClicksCount] {
        var lastClicksCountState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          lastClicksCountState = getRuntimeContext.getState(
            new ValueStateDescriptor[Long](
              "lastClicksCounter",
              classOf[Long]
            )
          )
        }

        override def processElement(
                                     click: Click,
                                     ctx: KeyedProcessFunction[String, Click, ClickWithLastClicksCount]#Context,
                                     out: Collector[ClickWithLastClicksCount]
                                   ): Unit = {

          val currentClickCount = click.clickCount

          val lastClickCount = lastClicksCountState.value()
          lastClicksCountState.update(currentClickCount)

          needToOutputClick(lastClickCount, currentClickCount) match {
            case true => out.collect(ClickWithLastClicksCount(click, lastClickCount))
            case false =>
          }
        }
      })

    clicksByUserStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
