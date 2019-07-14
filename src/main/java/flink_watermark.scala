import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object flink_watermark {

  //用户行为封装(用户id， 商品id， 商品类目ID， 行为类型， 时间戳)
  case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior: String, time: Long)
  //商品点击两封装(商品ID, 窗口结束时间戳, 商品的点击量)
  case class ItemViewCount(itemId:Long, windowEnd:Long, viewCount:Long)

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    // 告诉系统按照 EventTime 处理
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //读取绝对路径下的文件
    //val source = senv.readTextFile("file:///F://bigdata//FlinkPro//src//main//resources//UserBehavior.csv")
    //读取相对路径下的文件，即resources文件夹下的文件
    val path = flink_watermark.getClass.getClassLoader.getResource("UserBehavior2.csv")
    val source = senv.readTextFile(path.toString)
//    println(path) //output:file:/F:/bigdata/FlinkPro/target/classes/UserBehavior.csv

    //创建拥有UserBehavior结构的数据源
    val dataSource = source.map(value => {
      val columns = value.split(",")
      UserBehavior(columns(0).toLong, columns(1).toLong, columns(2).toInt, columns(3).toString, columns(4).toLong)
    })

    //指定如何获得业务时间，以及生成 Watermark
    val timedData:DataStream[UserBehavior] = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior] {
      override def extractAscendingTimestamp(behavior: UserBehavior): Long = {
         behavior.time*10000
      }
      /*val currentMaxTimestamp = 0
      val maxOutOfOrderness = 10000L//最大允许的乱序时间时10s
      val a:Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")//定义时间格式
    //获取watermark
    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }*/

    });

    //过滤出点击事件
    val pvData:DataStream[UserBehavior] = timedData.filter(new FilterFunction[UserBehavior] {
      override def filter(behavior: UserBehavior): Boolean = {
        behavior.behavior.equals("pv")
      }
    })



    pvData.print()

    senv.execute("watermark")
  }

  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    /*Tuple key,  // 窗口的主键，即 itemId
    TimeWindow window,  // 窗口
    Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
    Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
    */
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    }
  }
}
