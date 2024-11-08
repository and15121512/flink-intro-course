import module5.{Config, EventType}
import module5generator._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListStateDescriptor, MapState, MapStateDescriptor, State, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._

object Module5Task1App {

  val maxOutOfOrderness: Duration = java.time.Duration.ofMillis(1000L)
  private val installReportThreshold = 20
  private val uninstallReportThreshold = 10
  private val errorRateReportFreq = 2000L
  private val numberOfTopPlacesToReport = 3

  val configFileName = "/data-generator-config.json"
  val stores = Config.getConfig(configFileName).stores.keys.toSeq

  def getIndicatorOfEventType(event: Event, eventType: EventType): Int = {
    if (event.eventType == eventType) 1 else 0
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
          installCounterState.update(0)
          uninstallCounterState.update(0)
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

  class ErrorRateTrigger extends Trigger[Event, TimeWindow] {

    override def onElement(
                            element: Event,
                            timestamp: Long,
                            window: TimeWindow,
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
        if (errorCounterByStoreState.contains(element.store)) {
          val currCounter = errorCounterByStoreState.get(element.store)
          errorCounterByStoreState.put(element.store, currCounter + 1)
        }
        else {
          errorCounterByStoreState.put(element.store, 1)
        }
      }

      ctx.registerProcessingTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println(s"ERROR RATE TRIGGERED ON PROCESSING TIME !!!")
      TriggerResult.FIRE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println(s"ERROR RATE TRIGGERED ON EVENT TIME !!!")
      TriggerResult.CONTINUE
    }

    override def canMerge: Boolean = true

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
      val windowMaxTimestamp = window.maxTimestamp
      if (windowMaxTimestamp > ctx.getCurrentProcessingTime) ctx.registerProcessingTimeTimer(windowMaxTimestamp)
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteProcessingTimeTimer(window.maxTimestamp)
    }
  }



  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val eventStream: DataStreamSource[Event] = env.addSource(new EventGenerator)

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

    val errorRateReportOutputTag = new OutputTag[Event]("error-rate-report") {}

    val nonErrorEventStream = eventStreamWithWatermark
      .process(new ProcessFunction[Event, Event] {
        override def processElement(event: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
          getIndicatorOfEventType(event, EventType.Error) match {
            case 1 => ctx.output(errorRateReportOutputTag, event)
            case 0 => out.collect(event)
          }
        }
      })

    val installUninstallOutputStream = nonErrorEventStream
      .keyBy((event: Event) => event.store)
      .window(GlobalWindows.create())
      .trigger(new InstallUninstallTrigger)
      .process(new ProcessWindowFunction[Event, String, String, GlobalWindow] {

        var installCounterByAppIdState: MapState[String, Int] = _

        var uninstallCounterByAppIdState: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
          installCounterByAppIdState = getRuntimeContext.getMapState(
            new MapStateDescriptor(
              "install-counter-by-app-id",
              classOf[String],
              classOf[Int]
            )
          )
          uninstallCounterByAppIdState = getRuntimeContext.getMapState(
            new MapStateDescriptor(
              "uninstall-counter-by-app-id",
              classOf[String],
              classOf[Int]
            )
          )
        }

        def getTopInstallUninstallLog(
                                     store: String,
                                     topInstalled: List[(String, Int)],
                                     topUninstalled: List[(String, Int)]
                                     ) = {
          val nowTime = java.time.Instant.now().toString
          s"[${nowTime}] Store: ${store} " +
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

          installCounterByAppIdState.clear()
          uninstallCounterByAppIdState.clear()

          out.collect(getTopInstallUninstallLog(key, topAppsByInstall, topAppsByUninstall))
        }
      })

    val errorSideOutputEventStream: DataStream[Event] = nonErrorEventStream.getSideOutput(errorRateReportOutputTag)

    val errorOutputStream = errorSideOutputEventStream
      .windowAll(ProcessingTimeSessionWindows.withGap(Time.milliseconds(errorRateReportFreq)))
      .trigger(new ErrorRateTrigger)
      .process(new ProcessAllWindowFunction[Event, String, TimeWindow] {

        var errorCounterByStoreState: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
          errorCounterByStoreState = getRuntimeContext.getMapState(
            new MapStateDescriptor(
              "error-counter-by-store",
              classOf[String],
              classOf[Int]
            )
          )
        }

        /*def getErrorRateLog(store: String, errorCounter: Int): String = {
          val nowTime = java.time.Instant.now().toString
          s"[${nowTime}] Store: ${store} Error count: ${errorCounter}"
        }*/

        def getErrorRateLog(errorCounterByStore: Map[String, Int]): String = {
          val nowTime = java.time.Instant.now().toString
          s"[${nowTime}] ${errorCounterByStore.map(
            storeAndCnt => s"Store: ${storeAndCnt._1} Error count: ${storeAndCnt._2}"
          ).mkString(" ")}"
        }

        override def process(
                              context: ProcessAllWindowFunction[Event, String, TimeWindow]#Context,
                              elements: lang.Iterable[Event],
                              out: Collector[String]
                            ): Unit = {

          //val storeErrorCount = errorCounterByStoreState.get(store)
          //errorCounterByStoreState.remove(store)
          //out.collect(getErrorRateLog(store, storeErrorCount))

          val errorCounterByStore = stores.map( store =>
            (store, errorCounterByStoreState.get(store))
          ).toMap

          out.collect(getErrorRateLog(errorCounterByStore))
        }

      })

    installUninstallOutputStream.print()
    errorOutputStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }

}
