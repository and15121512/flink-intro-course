import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Module2Task2App {

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  // To make the logic more transparent
  case class LanguageToken(var language: String, var token: String) {
    def this() = {
      this("", "")
    }
  }

  def impl(): Unit = {

    val inputStream = env.fromElements(
      ("en", "car, bus, "),
      ("ru", "машина, "),
      ("en", "course, "),
      ("en", "house, "),
    )

    val langTokenInputStream = inputStream
      .map((rec: (String, String)) =>
        LanguageToken(rec._1, rec._2)
      )
      .returns(Types.POJO(classOf[LanguageToken]))

    val langTokenProcessedStream = langTokenInputStream
      .keyBy((langToken: LanguageToken) => langToken.language)
      .reduce(
        (langToken1: LanguageToken, langToken2: LanguageToken) =>
          LanguageToken(langToken1.language, langToken1.token ++ langToken2.token)
      )

    val langTokenOutputStream = langTokenProcessedStream
      .map((langToken: LanguageToken) =>
        langToken match {
          case LanguageToken (language, token) => (language, token)
          case _ =>
        }
      )

    langTokenOutputStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    impl()
  }

}
