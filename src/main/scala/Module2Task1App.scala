import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2

object Module2Task1App {

  def impl(): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val textStream: DataStreamSource[String] = env.fromElements(
      "The Apache Flink community is excited to announce the release of Flink ML!",
      "This release focuses on enriching Flink MLs feature engineering algorithms.",
      "The library now includes thirty three feature engineering algorithms,",
      "making it a more comprehensive library for feature engineering tasks."
    )

    val articles = textStream.flatMap((sentence: String, out: Collector[String]) => {
      sentence.toLowerCase().split("\\W+").filter(List("the", "a").contains(_)).foreach(out.collect)
    }).returns(Types.STRING)

    val result = articles
      .map(new MapFunction[String, Tuple2[String, Int]] {
        override def map(token: String): Tuple2[String, Int] = {
          new Tuple2[String, Int](token, 1)
        }
      })
      .keyBy((value: Tuple2[String, Int]) => value.f0 == "a")
      .reduce((tuple1: Tuple2[String, Int], tuple2: Tuple2[String, Int]) =>
        new Tuple2(tuple1.f0, tuple1.f1 + tuple2.f1))

    result.print()

    env.execute()

  }

  def main(args: Array[String]): Unit = {
    impl()
  }

}