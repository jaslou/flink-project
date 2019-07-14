package flink_kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08


object ReceiverFromKafka {

  private val ZOOKEEPER_HOST = "xxx.xxx.xxx.xxx:xxxx,xxx.xxx.xxx.xxx:xxx,xxx.xxx.xxx.xxx:xxxx"
  private val KAFKA_BROKER = "xxx.xxx.xxx.xxx:xxxx"
  private val GROUP_ID = "flink_group_test"


  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.enableCheckpointing(1000)
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //configure kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", GROUP_ID)

    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction = senv
      .addSource(
        new FlinkKafkaConsumer08[String]("flink_topic_test", new SimpleStringSchema(), kafkaProps)
      )
    transaction
      .map((_,1))
      .keyBy(0)
//      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum(1)
      .print()
//    transaction.print()
    senv.execute("kafka job")



  }
}
