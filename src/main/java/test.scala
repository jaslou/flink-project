import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Test {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    val benv = ExecutionEnvironment.getExecutionEnvironment
//    val dataB = benv.readTextFile("")
//    val wordCount = dataB.
//    wordCount.print()
//    benv.execute("batch")


    val data = senv.socketTextStream("192.168.244.10",9999)
    /*val wordCounts = data.map(_.toString)
      .map((_,1L))
      .keyBy(0)
      .sum(1)*/
    /*val wordCounts = data.map(_.toString)
        .map(x=>(WordCount(x, 1L)))
        .keyBy("word")
        .sum("count")
    wordCounts.print().setParallelism(1)//设置并行度，parallelism是指taskmanager实际使用的并发能力*/
    /*val wordCounts = data.map(_.toString)
      .map(x=>(WordCount(x, 1L)))
      .keyBy("word")
//      .timeWindow(Time.seconds(5),Time.seconds(1))
      .countWindowAll(5)//每5条数据输出一次，a,a,a,a,a,=>a;  a,b,b,b,b=>a;  1,a,a,a,a=>1
      .sum("count")*/
    //max
    val wordCounts = data.map(_.toString)
      .map(x=>(WordCount(x.split(",")(0),x.split(",")(1).toLong)))//输入格式为:a,10;b,2
      .keyBy("word")
      .max("count")
      //      .timeWindow(Time.seconds(5),Time.seconds(1))
//      .countWindowAll(5)//每5条数据输出一次，a,a,a,a,a,=>a;  a,b,b,b,b=>a;  1,a,a,a,a=>1
//      .sum("count")

    wordCounts.print().setParallelism(1)//设置并行度，parallelism是指taskmanager实际使用的并发能力
    senv.execute("Stream")
  }
  case class WordCount(word:String, count:Long)
}
