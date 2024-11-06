import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._

object Module3Task2App {

  final case class Booking(geoRegion: String, eventTime: Instant, bookingCount: Int)

  val startTime: Instant =
    Instant.parse("2023-07-15T00:00:00.000Z")

  val eventTime = (millis: Long) => startTime.plusMillis(millis)

  val clicks = List(
    Booking("Domestic", eventTime(1000L), 4),
    Booking("International", eventTime(1000L), 6),
    Booking("Domestic", eventTime(1000L), 10),
    Booking("International", eventTime(1200L), 7),
    Booking("International", eventTime(1800L), 12),
    Booking("International", eventTime(1500L), 4),
    Booking("International", eventTime(2000L), 1),
    Booking("Domestic", eventTime(2100L), 0),
    Booking("Domestic", eventTime(3000L), 2),
    Booking("International", eventTime(6000L), 8),
    Booking("International", eventTime(6000L), 1),
    Booking("Domestic", eventTime(6700L), 1),
    Booking("International", eventTime(7200L), 5),
    Booking("Domestic", eventTime(8000L), 3),
    Booking("International", eventTime(8100L), 6),
    Booking("Domestic", eventTime(8400L), 14),
    Booking("International", eventTime(9000L), 2),
    Booking("International", eventTime(9000L), 4),
  ).asJava

  val maxOutOfOrderness = java.time.Duration.ofMillis(1000L)
  val slidingWindowSize = Time.seconds(2)
  val slidingWindowSlide = Time.seconds(1)

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  //// QUESTION ???
  //
  // Is it possible to use AggregateFunction here (aggregate() instead of apply())
  // to use incremental aggregation and still be able to obtain window bounds?
  // As I understand it, apply requires full data arrival to start processing.
  // On the other hand, AggregateFunction has no TimeWindow metadata to obtain window bounds
  // (window bounds logging is required by the task).
  //
  //// QUESTION ???
  class bookingMinMaxFuntion extends WindowFunction[Booking, String, String, TimeWindow] {

    def getMinBooking(bookings: lang.Iterable[Booking]): Option[Int] = {
      bookings.asScala.toSeq.map(_.bookingCount).minOption
    }

    def getMaxBooking(bookings: lang.Iterable[Booking]): Option[Int] = {
      bookings.asScala.toSeq.map(_.bookingCount).maxOption
    }

    def getBookingMinMaxLog(geoRegion: String, bookings: lang.Iterable[Booking], window: TimeWindow): String = {
      s"Window [${window.getStart} - ${window.getEnd}] Type ${geoRegion} bookings count: " +
      s"Min=${getMinBooking(bookings).getOrElse("No bookings")} " +
      s"Max=${getMaxBooking(bookings).getOrElse("No bookings")}."
    }

    override def apply(
                        geoRegion: String,
                        window: TimeWindow,
                        bookings: lang.Iterable[Booking],
                        out: Collector[String]): Unit = {
      out.collect(getBookingMinMaxLog(geoRegion, bookings, window))
    }

  }

  def impl(): Unit = {

    val bookingStream = env
      .fromCollection(clicks)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(maxOutOfOrderness)
          .withTimestampAssigner(new SerializableTimestampAssigner[Booking] {
            override def extractTimestamp(element: Booking, recordTimestamp: Long): Long = {
              element.eventTime.toEpochMilli
            }
          })
      )
      .keyBy((booking: Booking) => booking.geoRegion)
      .window(SlidingEventTimeWindows.of(slidingWindowSize, slidingWindowSlide))
      .apply(new bookingMinMaxFuntion)

    bookingStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
