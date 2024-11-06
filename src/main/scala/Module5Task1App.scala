import module5.EventType
import module5generator._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

import java.time.{Duration, Instant}

object Module5Task1App {

  val maxOutOfOrderness: Duration = java.time.Duration.ofMillis(1000L)
  private val installThreshold = 100
  private val uninstallThreshold = 50

  class InstallUninstallTrigger extends Trigger[Event, GlobalWindow] {

    override def onElement(
                            element: Event,
                            timestamp: Long,
                            window: GlobalWindow,
                            ctx: Trigger.TriggerContext
                          ): TriggerResult = {
      val installCounterState = ctx.getPartitionedState(
        new ValueStateDescriptor(
          "install-counter",
          classOf[Int]
        )
      )
      val uninstallCounterState = ctx.getPartitionedState(
        new ValueStateDescriptor(
          "uninstall-counter",
          classOf[Int]
        )
      )
      val installCounterByAppIdState = ctx.getPartitionedState(
        new MapStateDescriptor(
          "install-counter-by-app-id",
          classOf[String],
          classOf[Int]
        )
      )
      val uninstallCounterByAppIdState = ctx.getPartitionedState(
        new MapStateDescriptor(
          "uninstall-counter-by-app-id",
          classOf[String],
          classOf[Int]
        )
      )

      if (getIndicatorOfEventType(element, EventType.Install) == 1) {
        if (installCounterByAppIdState.contains(element.appId)) {
          val currCounter = installCounterByAppIdState.get(element.appId)
          installCounterByAppIdState.put(element.appId, currCounter + 1)
        }
        else {
          installCounterByAppIdState.put(element.appId, 1)
        }
      }

      if (getIndicatorOfEventType(element, EventType.Uninstall) == 1) {
        if (uninstallCounterByAppIdState.contains(element.appId)) {
          val currCounter = uninstallCounterByAppIdState.get(element.appId)
          uninstallCounterByAppIdState.put(element.appId, currCounter + 1)
        }
        else {
          uninstallCounterByAppIdState.put(element.appId, 1)
        }
      }

      val lastInstallCounter = installCounterState.value()
      val lastUninstallCounter = uninstallCounterState.value()

      val installIndicator = getIndicatorOfEventType(element, EventType.Install)
      val currentInstallCounter = lastInstallCounter + installIndicator

      val uninstallIndicator = getIndicatorOfEventType(element, EventType.Uninstall)
      val currentUninstallCounter = lastUninstallCounter + uninstallIndicator

      fireAndPurgeCriterion(currentInstallCounter, currentUninstallCounter) match {
        case true =>
          installCounterState.update(installIndicator)
          uninstallCounterState.update(uninstallIndicator)
          TriggerResult.FIRE_AND_PURGE
        case false =>
          installCounterState.update(currentInstallCounter)
          uninstallCounterState.update(currentUninstallCounter)
          TriggerResult.CONTINUE
      }
    }

    private def getIndicatorOfEventType(event: Event, eventType: EventType) = {
      val currEventType = EventType.getByName(event.eventType) match {
        case Some(eventType) => eventType
        case None => throw new RuntimeException(s"Invalid EventType found: `${event.eventType}`")
      }
      if (currEventType == eventType) 1 else 0
    }

    private def fireAndPurgeCriterion(
                                      currentInstallCounter: Int,
                                      currentUninstallCounter: Int
                                    ) = {
      currentInstallCounter >= installThreshold || currentUninstallCounter >= uninstallThreshold
    }

    override def onProcessingTime(
                                   time: Long,
                                   window: GlobalWindow,
                                   ctx: Trigger.TriggerContext
                                 ): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(
                              time: Long,
                              window: GlobalWindow,
                              ctx: Trigger.TriggerContext
                            ): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(
                        window: GlobalWindow,
                        ctx: Trigger.TriggerContext
                      ): Unit = {
      TriggerResult.PURGE
    }
  }

  class ErrorRateTrigger extends Trigger[Event, GlobalWindow] {

    override def onElement(
                            element: Event,
                            timestamp: Long,
                            window: GlobalWindow,
                            ctx: Trigger.TriggerContext
                          ): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(
                                   time: Long,
                                   window: GlobalWindow,
                                   ctx: Trigger.TriggerContext
                                 ): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(
                              time: Long,
                              window: GlobalWindow,
                              ctx: Trigger.TriggerContext
                            ): TriggerResult = {
      val errorCounterState = ctx.getPartitionedState(
        new ValueStateDescriptor(
          "error-counter",
          classOf[Int]
        )
      )

      val lastFiredTimeState = ctx.getPartitionedState(
        new ValueStateDescriptor(
          "last-fired-time",
          classOf[Instant]
        )
      )

      val lastErrorCounter = errorCounterState.value()
      val lastFiredTime = lastFiredTimeState.value()

      ???

    }

    def fireAndPurgeCriterion(lastErrorCounter: Int, ) = {}

    override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
      TriggerResult.PURGE
    }
  }

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")
  val eventStream: DataStreamSource[Event] = env.addSource(new EventGenerator(1, startTime))

  def main(args: Array[String]): Unit = {
    val eventStreamWithWatermark = eventStream.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(maxOutOfOrderness)
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )

    // Output tag for error rates estimation

    eventStreamWithWatermark
      .keyBy((event: Event) => event.store)
      .window(GlobalWindows.create())
      .trigger(new InstallUninstallTrigger)

  }

}
