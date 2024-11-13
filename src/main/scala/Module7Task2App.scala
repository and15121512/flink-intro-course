import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._

object Module7Task2App {

  val lateArriveThreshold = Duration.ofDays(3)
  val finishProcessingDelayTime = Time.seconds(30)

  case class Order(
                  id: String,
                  orderTime: String
                  )

  case class Delivery(
                     id: String,
                     orderId: String,
                     deliveryTime: String
                     )

  case class DeliveryReport(
                           deliveryId: String,
                           orderId: String,
                           deadlineDuration: Duration,
                           deliveryDuration: Duration
                           )

  val orders = List(
    Order("675e3421", "2023-06-15T12:07:13.000Z"),
    Order("267e9361", "2023-06-19T23:15:44.000Z"),
    Order("267e8761", "2023-06-03T14:35:05.000Z"),
    Order("267e9061", "2023-07-01T00:18:34.000Z"),
    Order("452e7169", "2023-06-30T13:55:21.000Z"),
  ).asJava

  val deliveries = List(
    Delivery("d923e0511","675e3421", "2023-06-16T14:11:54.000Z"),
    Delivery("d876e0521","267e9361", "2023-06-25T18:04:40.000Z"),
    Delivery("d653e1241","267e8761", "2023-06-05T17:17:36.000Z"),
    Delivery("d923e0511","267e9061", "2023-07-04T02:18:34.000Z"),
    Delivery("d745e1941","452e7169", "2023-06-02T03:42:11.000Z"),
  ).asJava

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  class DeliveryLatenessStatsFunction extends CoProcessFunction[Order, Delivery, String] {

    var actualOrdersState: MapState[String, String] = _
    var lateDeliveriesCounterState: ValueState[Int] = _
    var inTimeDeliveriesCounterState: ValueState[Int] = _
    var inTimeDeliveryReportsState: ListState[DeliveryReport] = _

    override def open(parameters: Configuration): Unit = {
      actualOrdersState = getRuntimeContext.getMapState(
        new MapStateDescriptor[String, String](
          "actual-orders-state",
          classOf[String],
          classOf[String]
        )
      )

      lateDeliveriesCounterState = getRuntimeContext.getState(
        new ValueStateDescriptor[Int](
          "late-deliveries-counter",
          classOf[Int]
        )
      )

      inTimeDeliveriesCounterState = getRuntimeContext.getState(
        new ValueStateDescriptor[Int](
          "in-time-deliveries-counter",
          classOf[Int]
        )
      )

      inTimeDeliveryReportsState = getRuntimeContext.getListState(
        new ListStateDescriptor[DeliveryReport](
          "in-time-delivery-reports",
          classOf[DeliveryReport]
        )
      )
    }

    override def processElement1(
                                  order: Order,
                                  ctx: CoProcessFunction[Order, Delivery, String]#Context,
                                  out: Collector[String]
                                ): Unit = {
      if (!actualOrdersState.contains(order.id)) {
        actualOrdersState.put(order.id, order.orderTime)
      }
      out.collect(s"Order ${order} arrived")
    }

    override def processElement2(
                                  delivery: Delivery,
                                  ctx: CoProcessFunction[Order, Delivery, String]#Context,
                                  out: Collector[String]
                                ): Unit = {
      /////
      // TODO: NOT CORRECT !!! ORDER EVENTS COULD ARRIVE LATELY TOO !!!
      /////
      if (!actualOrdersState.contains(delivery.orderId)) {
        throw new RuntimeException(s"Invalid input: delivery ${delivery} has no matching order record.")
      }

      val orderTime = actualOrdersState.get(delivery.orderId)
      val deliveryTime = delivery.deliveryTime

      val deliveryLateness = getDeliveryLateness(orderTime, deliveryTime)
      if (lateDeliveryCondition(deliveryLateness)) {
        val currLateDeliveriesCounter = lateDeliveriesCounterState.value()
        lateDeliveriesCounterState.update(currLateDeliveriesCounter + 1)
      }
      else {
        inTimeDeliveryReportsState.add(
          DeliveryReport(
            delivery.id,
            delivery.orderId,
            getDeliveryDeadline(orderTime),
            deliveryLateness
          )
        )
        val inTimeDeliveriesCounter = inTimeDeliveriesCounterState.value()
        inTimeDeliveriesCounterState.update(inTimeDeliveriesCounter + 1)
      }


      out.collect(s"Delivery ${delivery} arrived")
    }

    private def getDeliveryLateness(orderTimeStr: String, deliveryTimeStr: String): Duration = {
      val orderTime = castStrToEventTime(orderTimeStr)
      val deliveryTime = castStrToEventTime(deliveryTimeStr)
      if (orderTime.isAfter(deliveryTime)) {
        throw new RuntimeException(
          s"Invalid input: delivery (${deliveryTimeStr}) happen before ordering (${orderTimeStr})."
        )
      }
      Duration.between(orderTime, deliveryTime)
    }

    private def castStrToEventTime(timeStr: String): Instant = {
      Instant.parse(timeStr)
    }

    private def lateDeliveryCondition(deliveryLateness: Duration): Boolean = {
      deliveryLateness.compareTo(lateArriveThreshold) > 0
    }

    private def getDeliveryDeadline(orderTimeStr: String): Duration = {
      lateArriveThreshold
    }
  }

  def impl(): Unit = {

    val inputOrdersStream = env.fromCollection(orders)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .noWatermarks()
      )

    val inputDeliveriesStream = env.fromCollection(deliveries)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .noWatermarks()
      )

    inputOrdersStream
      .connect(inputDeliveriesStream)
      .process(new DeliveryLatenessStatsFunction)
      .windowAll(ProcessingTimeSessionWindows.withGap(finishProcessingDelayTime))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {

        var actualOrdersState: MapState[String, String] = _
        var lateDeliveriesCounterState: ValueState[Int] = _
        var inTimeDeliveriesCounterState: ValueState[Int] = _
        var inTimeDeliveryReportsState: ListState[DeliveryReport] = _

        override def open(parameters: Configuration): Unit = {
          lateDeliveriesCounterState = getRuntimeContext.getState(
            new ValueStateDescriptor[Int](
              "late-deliveries-counter",
              classOf[Int]
            )
          )

          inTimeDeliveriesCounterState = getRuntimeContext.getState(
            new ValueStateDescriptor[Int](
              "in-time-deliveries-counter",
              classOf[Int]
            )
          )

          inTimeDeliveryReportsState = getRuntimeContext.getListState(
            new ListStateDescriptor[DeliveryReport](
              "in-time-delivery-reports",
              classOf[DeliveryReport]
            )
          )
        }

        override def process(
                              context: ProcessAllWindowFunction[String, String, TimeWindow]#Context,
                              elements: lang.Iterable[String],
                              out: Collector[String]
                            ): Unit = {
          out.collect(s"${}")
        }
      })
  }

  def main(args: Array[String]): Unit = {
    impl()
  }
}
