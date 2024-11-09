import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, State, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala

object Module5Task2App {

  val maxBoundedOutOfOrderness = java.time.Duration.ofMillis(1000L)
  val windowBatchTime = Time.milliseconds(10000L)
  val hardwareTopicName = "Hardware"

  case class TimeOfDay(
                        startHoursFromMidnight: Long,
                        endHoursFromMidnight: Long
                      ) {
    def isHourWithinTimeOfDay(hour: Long): Boolean = {
      hour >= startHoursFromMidnight && hour < endHoursFromMidnight
    }
  }

  object TimeOfDay {

    val Mourning = TimeOfDay(4, 12)
    val Day = TimeOfDay(12, 17)
    val Evening = TimeOfDay(17, 24)
    val Midnight = TimeOfDay(0, 4)

    def getByInstant(currTime: java.time.Instant): TimeOfDay = {
      val millisInDay = 60 * 60 * 24 * 1000
      val dayStart = java.time.Instant.ofEpochMilli((currTime.toEpochMilli / millisInDay) * millisInDay)
      val hoursFromMidnight = java.time.Duration.between(dayStart, currTime).toHours
      if (Mourning.isHourWithinTimeOfDay(hoursFromMidnight)) Mourning
      else if (Day.isHourWithinTimeOfDay(hoursFromMidnight)) Day
      else if (Evening.isHourWithinTimeOfDay(hoursFromMidnight)) Evening
      else if (Mourning.isHourWithinTimeOfDay(hoursFromMidnight)) Midnight
      else throw new RuntimeException("Definition of TimeOfDay is invalid")
    }
  }


  val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")

  case class CommentInput(
                     @JsonProperty("commentId") var commentId: String,
                     @JsonProperty("time") var time: Long,
                     @JsonProperty("user") var user: String,
                     @JsonProperty("topic") var topic: String,
                     @JsonProperty("acronim") var acronim: String
                   )

  case class Comment(
                      commentId: String,
                      user: String,
                      topic: String,
                      acronim: String,
                      time: java.time.Instant
                    )

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  def impl() = {

    val filePath = new Path("src/main/resources/acronims.csv")

    val csvSchema = CsvSchema
      .builder()
      .addColumn("commentId")
      .addNumberColumn("time")
      .addColumn("user")
      .addColumn("topic")
      .addColumn("acronim")
      .build()

    val source: FileSource[CommentInput] = FileSource
      .forRecordStreamFormat(
        CsvReaderFormat.forSchema(csvSchema, Types.GENERIC(classOf[CommentInput])),
        filePath
      )
      .build()

    val hardwareOutputTag = new OutputTag[CommentInput]("hardware-output-tag") {}

    val commentInputStream = env.fromSource(
      source,
      WatermarkStrategy
        .forBoundedOutOfOrderness(maxBoundedOutOfOrderness)
        .withTimestampAssigner(new SerializableTimestampAssigner[CommentInput] {
          override def extractTimestamp(element: CommentInput, recordTimestamp: Long): Long = {
            element.time
          }
        }),
      "comments-csv"
    )
      .process(new ProcessFunction[CommentInput, Comment] {
        override def processElement(
                                     commentInput: CommentInput,
                                     ctx: ProcessFunction[CommentInput, Comment]#Context,
                                     out: Collector[Comment]
                                   ): Unit = {

          val comment = Comment(
            commentInput.commentId,
            commentInput.user,
            commentInput.topic,
            commentInput.acronim,
            startTime.plus(java.time.Duration.ofMillis(commentInput.time))
          )

          if (comment.topic == hardwareTopicName) {
            ctx.output(hardwareOutputTag, comment)
          }
          out.collect(comment)
        }
      })

    commentInputStream
      .keyBy((comment: Comment) => comment.topic)
      //////
      // Keyed by topic to provide analytics by topic.
      // Assume that topic column has low cardinality (so it is reasonable to key by topic).
      //////
      .window(TumblingEventTimeWindows.of(windowBatchTime))
      //////
      // Use tumbling window function to batch events.
      // Just to avoid using ValueState on every event arrived.
      // Don't use window for the whole TimeOfDay due to its long duration (otherwise it is
      // possible to have thousands or even millions of events within one window).
      //////
      .process(new ProcessWindowFunction[Comment, Comment, String, TimeWindow] {

        //////
        // For each topic and time of day calculate number of users and acronims
        // as statistics required by the task.
        //////
        var commentUsersCounterState: ValueState[Int] = _
        var commentAcronimsCounterState: MapState[String, Int] = _
        var lastTimeOfDayState: ValueState[TimeOfDay] = _

        override def open(parameters: Configuration): Unit = {
          commentUsersCounterState = getRuntimeContext
            .getState(
              new ValueStateDescriptor[Int](
                "comment-users-counter",
                classOf[Int]
              )
            )
          commentAcronimsCounterState = getRuntimeContext
            .getMapState(
              new MapStateDescriptor[String, Int](
                "comment-acronims-counter",
                classOf[String],
                classOf[Int]
              )
            )
          lastTimeOfDayState = getRuntimeContext
            .getState(
              new ValueStateDescriptor[TimeOfDay](
                "last-time-of-day",
                classOf[TimeOfDay]
              )
            )
        }

        override def process(
                              topic: String,
                              context: ProcessWindowFunction[Comment, Comment, String, TimeWindow]#Context,
                              comments: lang.Iterable[Comment],
                              out: Collector[Comment]
                            ): Unit = {
          //////
          // It is possible for the window to intersect time of day bound (e.g. night and mourning bound).
          // In that case we assign the day of time for that window based on min event time within that window.
          // In our example, if min event time is lower than the bound, the window is night.
          // If min event type is greater than the bound, the window is mourning.
          //////
          val minCommentTime = comments.asScala.map(_.time).min

          val currentTimeOfDay = TimeOfDay.getByInstant(minCommentTime)
          val lastTimeOfDay = lastTimeOfDayState.value()

          if (currentTimeOfDay != lastTimeOfDay) {
            comments.asScala.map(comment => commentAcronimsCounterState.get(comment.acronim))
            // ...
          }

        }
      })

  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
