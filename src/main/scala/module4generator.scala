import org.apache.flink.streaming.api.functions.source.SourceFunction


package object module4generator {

  case class Click(
                    userId: String,
                    button: String,
                    clickCount: Int,
                    time: java.time.Instant)

  class ClickGenerator(
                        batchSize: Int,
                        baseTime: java.time.Instant,
                        millisBtwEvents: Int)
    extends SourceFunction[Click] {

    @volatile private var isRunning = true

    private def generateClick(id: Long): Seq[Click] = {
      val events = (1 to batchSize)
        .map(_ =>
          Click(
            ClickGenerator.getUserId,
            ClickGenerator.getButton,
            ClickGenerator.getClickCount,
            baseTime.plusSeconds(id)
          )
        )

      events
    }


    private def run(
                     startId: Long,
                     ctx: SourceFunction.SourceContext[Click])
    : Unit = {

      while (isRunning) {
        generateClick(startId).foreach(ctx.collect)
        Thread.sleep(batchSize * millisBtwEvents)
        run(startId + batchSize, ctx)
      }
    }


    override def run(ctx: SourceFunction.SourceContext[Click]): Unit = run(0, ctx)

    override def cancel(): Unit = isRunning = false
  }

  object ClickGenerator {

    private val buttons: Vector[String] = Vector("GreenBtn", "BlueBtn", "RedBtn", "OrangeBtn")

    private val users: Vector[String] = Vector(
      "65b4c326",
      "1b2e622d",
      "24f3c8b8",
      "9a608d9e")

    def getButton: String = buttons(scala.util.Random.nextInt(buttons.length))

    def getUserId: String = users(scala.util.Random.nextInt(users.length))

    def getClickCount: Int = scala.util.Random.nextInt(10)
  }

}