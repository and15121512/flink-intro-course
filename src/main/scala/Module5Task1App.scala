import module5.{Config, EventType}
import module5generator._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._

object Module5Task1App {

  val maxOutOfOrderness: Duration = java.time.Duration.ofMillis(1000L)
  private val installReportThreshold = 100
  private val uninstallReportThreshold = 50
  private val errorRateReportFreq = java.time.Duration.ofMillis(10000L)
  private val numberOfTopPlacesToReport = 3

  val configFileName = "data-generator-config.json"
  val stores = Config.getConfig(configFileName).stores.keys.toSeq

  def getIndicatorOfEventType(event: Event, eventType: EventType): Int = {
    val currEventType = EventType.getByName(event.eventType) match {
      case Some(eventType) => eventType
      case None => throw new RuntimeException(s"Invalid EventType found: `${event.eventType}`")
    }
    if (currEventType == eventType) 1 else 0
  }

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
          installCounterByAppIdState.keys().asScala.foreach(store => {
            installCounterByAppIdState.remove(store)
          })
          uninstallCounterByAppIdState.keys().asScala.foreach(store => {
            uninstallCounterByAppIdState.remove(store)
          })
          TriggerResult.FIRE_AND_PURGE
        case false =>
          installCounterState.update(currentInstallCounter)
          uninstallCounterState.update(currentUninstallCounter)
          TriggerResult.CONTINUE
      }
    }

    private def fireAndPurgeCriterion(
                                      currentInstallCounter: Int,
                                      currentUninstallCounter: Int
                                    ) = {
      currentInstallCounter >= installReportThreshold || currentUninstallCounter >= uninstallReportThreshold
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
      val errorCounterByStoreState = ctx.getPartitionedState(
        new MapStateDescriptor(
          "error-counter-by-store",
          classOf[String],
          classOf[Int]
        )
      )

      if (getIndicatorOfEventType(element, EventType.Error) == 1) {
        if (errorCounterByStoreState.contains(element.appId)) {
          val currCounter = errorCounterByStoreState.get(element.appId)
          errorCounterByStoreState.put(element.appId, currCounter + 1)
        }
        else {
          errorCounterByStoreState.put(element.appId, 1)
        }
      }

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
      val errorCounterByStoreState = ctx.getPartitionedState(
        new MapStateDescriptor(
          "error-counter-by-store",
          classOf[String],
          classOf[Int]
        )
      )

      val lastFiredTimeState = ctx.getPartitionedState(
        new ValueStateDescriptor(
          "last-fired-time",
          classOf[Instant]
        )
      )

      val lastFiredTime = lastFiredTimeState.value()

      val nowTime = java.time.Instant.now()
      nowTime.isAfter(lastFiredTime.plus(errorRateReportFreq)) match {
        case true =>
          lastFiredTimeState.update(nowTime)
          errorCounterByStoreState.keys().asScala.foreach(store => {
            errorCounterByStoreState.remove(store)
          })
          TriggerResult.FIRE_AND_PURGE
        case false =>
          TriggerResult.CONTINUE
      }
    }

    override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
      TriggerResult.PURGE
    }
  }


  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val startTime: Instant = Instant.parse("2023-07-15T00:00:00.000Z")
  val eventStream: DataStreamSource[Event] = env.addSource(new EventGenerator(1, startTime))

  def impl(): Unit = {

    val eventStreamWithWatermark = eventStream.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(maxOutOfOrderness)
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )

    val errorRateReportOutputTag = new OutputTag[String]("error-rate-report") {}

    val nonErrorEventStream = eventStream
      .process(new ProcessFunction[Event, Event] {
        override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
          getIndicatorOfEventType(event, EventType.Error) match {
            case 1 => ctx.output(errorRateReportOutputTag, event)
            case 0 => out.collect(event)
          }
        }
      })

    nonErrorEventStream
      .keyBy((event: Event) => event.store)
      .window(GlobalWindows.create())
      .trigger(new InstallUninstallTrigger)
      .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {

        val installCounterByAppIdState = getRuntimeContext.getMapState(
          new MapStateDescriptor(
            "install-counter-by-app-id",
            classOf[String],
            classOf[Int]
          )
        )
        val uninstallCounterByAppIdState = getRuntimeContext.getMapState(
          new MapStateDescriptor(
            "uninstall-counter-by-app-id",
            classOf[String],
            classOf[Int]
          )
        )

        def getTopInstallUninstallLog(
                                     store: String,
                                     topInstalled: List[(String, Int)],
                                     topUninstalled: List[(String, Int)]
                                     ) = {
          val nowTime = java.time.Instant.now().toString
          s"[${nowTime}] Store: ${store}" +
            s"Top ${topInstalled.size} installed: ${
              topInstalled.map(appAndCnt => s"${appAndCnt._1} (${appAndCnt._2})").mkString(", ")
            } " +
            s"Top ${topUninstalled.size} uninstalled: ${
              topUninstalled.map(appAndCnt => s"${appAndCnt._1} (${appAndCnt._2})").mkString(", ")
            } "
        }

        override def process(
                              key: String,
                              context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
                              elements: lang.Iterable[Event],
                              out: Collector[String]
                            ): Unit = {

          val topAppsByInstall =
            installCounterByAppIdState.keys.asScala.map( appId =>
              (appId, installCounterByAppIdState.get(appId))
            ).toSeq
            .sortBy(-_._2)
            .take(numberOfTopPlacesToReport)
            .toList
          val topAppsByUninstall =
            uninstallCounterByAppIdState.keys.asScala.map( appId =>
                (appId, uninstallCounterByAppIdState.get(appId))
              ).toSeq
              .sortBy(-_._2)
              .take(numberOfTopPlacesToReport)
              .toList

          getTopInstallUninstallLog(key, topAppsByInstall, topAppsByUninstall)
        }

        // TODO: PROCESS SIDE OUTPUT (ERRORS) TOO !!!
        
      })
  }

  def main(args: Array[String]): Unit = {
    impl()
  }

}
