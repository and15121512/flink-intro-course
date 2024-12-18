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

  val maxBoundedOutOfOrderness = java.time.Duration.ofMillis(10000L)
  val windowBatchTime = Time.milliseconds(10000L)
  /*val hardwareTopicName = "Hardware"*/
  val topAcronymsToOutput = 20

  case class TimeOfDay(
                        startHoursFromMidnight: Long,
                        endHoursFromMidnight: Long,
                        stringRepr: String
                      ) {
    def isHourWithinTimeOfDay(hour: Long): Boolean = {
      hour >= startHoursFromMidnight && hour < endHoursFromMidnight
    }
  }

  object TimeOfDay {

    val Mourning = TimeOfDay(4, 12, "Mourning")
    val Day = TimeOfDay(12, 17, "Day")
    val Evening = TimeOfDay(17, 24, "Evening")
    val Midnight = TimeOfDay(0, 4, "Midnight")

    def getByInstant(currTime: java.time.Instant): TimeOfDay = {
      val millisInDay = 60 * 60 * 24 * 1000
      val dayStart = java.time.Instant.ofEpochMilli((currTime.toEpochMilli / millisInDay) * millisInDay)
      val hoursFromMidnight = java.time.Duration.between(dayStart, currTime).toHours
      if (Mourning.isHourWithinTimeOfDay(hoursFromMidnight)) Mourning
      else if (Day.isHourWithinTimeOfDay(hoursFromMidnight)) Day
      else if (Evening.isHourWithinTimeOfDay(hoursFromMidnight)) Evening
      else if (Midnight.isHourWithinTimeOfDay(hoursFromMidnight)) Midnight
      else throw new RuntimeException(s"Definition of TimeOfDay is invalid" +
        s"(currTime: ${currTime}, dayStart: ${dayStart}), hoursFromMidnight: ${hoursFromMidnight}")
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

  case class AcronymStats(
                           topic: String,
                           acronim: String,
                           acronymsCounter: Long,
                           totalCommentsCounter: Long,
                           timeOfDay: String,
                           time: java.time.Instant
                         )

  //////
  //case class HardwareStats(
  //                          totalCommentsCounter: Long,
  //                          time: java.time.Instant
  //                        )
  //////

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  def impl() = {

    val filePath = new Path("src/main/resources/acronyms_input.csv")

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

    //////
    // Side output is not required in our case because we calculate total comments
    // counter for every topic, not just "Hardware".
    // Here commented version of output tag provided to show its possible implementation
    // (in case it is necessary by the task).
    //////
    /*val hardwareOutputTag = new OutputTag[Comment]("hardware-output-tag") {}*/

    val commentInputStream = env.fromSource(
        source,
        //////
        // Due to no orderliness of input file it was sorted by time in advance.
        // Otherwise, I assume it doesn't make sense to use Flink as Batch processing
        // tool. If batch processing is required, one could use (for example) TimeSessionWindow
        // by processing time with large session time lag. This way state backend could be processed
        // in the same way with TumblingWindow. When batch processing ends (session closes) all stats will
        // be outputted at once.
        //////
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

          //////
          // Not required in our case, view the comment above.
          //////
          /*if (comment.topic == hardwareTopicName) {
            ctx.output(hardwareOutputTag, comment)
          }*/
          out.collect(comment)
        }
      })

    val acronymsOutputStream = commentInputStream
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
      .process(new ProcessWindowFunction[Comment, AcronymStats, String, TimeWindow] {

        //////
        // For each topic and time of day calculate number of users and acronyms (top N)
        // as statistics required by the task.
        //////
        var commentsCounterState: ValueState[Int] = _
        var commentAcronymsCounterState: MapState[String, Int] = _
        var lastTimeOfDayState: ValueState[TimeOfDay] = _

        override def open(parameters: Configuration): Unit = {
          commentsCounterState = getRuntimeContext
            .getState(
              new ValueStateDescriptor[Int](
                "comment-users-counter",
                classOf[Int]
              )
            )
          commentAcronymsCounterState = getRuntimeContext
            .getMapState(
              new MapStateDescriptor[String, Int](
                "comment-acronyms-counter",
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
                              context: ProcessWindowFunction[Comment, AcronymStats, String, TimeWindow]#Context,
                              comments: lang.Iterable[Comment],
                              out: Collector[AcronymStats]
                            ): Unit = {
          //////
          // It is possible for the window to intersect time of day bound (e.g. night and mourning bound).
          // In that case we assign the day of time for that window based on min event time within that window.
          // In our example, if min event time is lower than the bound, the window is night.
          // If min event type is greater than the bound, the window is mourning.
          //////
          val minCommentTime = comments.asScala.map(_.time).min

          val currentTimeOfDay = TimeOfDay.getByInstant(minCommentTime)

          val isInitial = lastTimeOfDayState.value() == null
          val lastTimeOfDay = lastTimeOfDayState.value()

          val currAcronymsCounter = comments.asScala
            .map(_.acronim)
            .groupMapReduce(identity)(_ => 1)(_ + _)

          if (currentTimeOfDay != lastTimeOfDay) {

            val topAcronyms = commentAcronymsCounterState.keys.asScala.map(appId =>
                (appId, commentAcronymsCounterState.get(appId))
              ).toSeq
              .sortBy(-_._2)
              .take(topAcronymsToOutput)
              .toList

            if (!isInitial) {
              topAcronyms.foreach(acronymAndCount => {
                out.collect(
                  AcronymStats(
                    topic,
                    acronymAndCount._1,
                    acronymAndCount._2,
                    commentsCounterState.value(),
                    currentTimeOfDay.stringRepr,
                    minCommentTime
                  )
                )
              })
            }

            commentAcronymsCounterState.clear()
            currAcronymsCounter.foreach(acronymAndCnt => {
              commentAcronymsCounterState.put(acronymAndCnt._1, acronymAndCnt._2)
            })
            commentsCounterState.update(comments.asScala.size)

            lastTimeOfDayState.update(currentTimeOfDay)
          }
          else {
            currAcronymsCounter.foreach(acronymAndCnt => {
              val currAcronymsCnt = commentAcronymsCounterState.get(acronymAndCnt._1)
              commentAcronymsCounterState.put(acronymAndCnt._1, currAcronymsCnt + acronymAndCnt._2)
            })
            val currUsersCnt = commentsCounterState.value()
            commentsCounterState.update(currUsersCnt + comments.asScala.size)
          }
        }
      })

    //////
    // val hardwareOutputStream = commentInputStream
    //   .getSideOutput(hardwareOutputTag)
    //     ...
    //     ...
    //////

    acronymsOutputStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
