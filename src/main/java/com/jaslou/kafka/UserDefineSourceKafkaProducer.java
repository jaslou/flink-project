package com.jaslou.kafka;

import akka.remote.serialization.ProtobufSerializer;
import com.jaslou.assigner.UserBehaviorTimeAssigner;
import com.jaslou.domin.UserBehavior;
import com.jaslou.domin.UserBehaviorSerializationSchema;
import com.jaslou.kafka.MyPartitioner;
import com.jaslou.source.UserBehaviorSource;
import com.jaslou.util.PropertyUtil;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;


import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * send data to kafka
 */
public class UserDefineSourceKafkaProducer {

    public static final String KAFKA_TOPIC = "flink_topic";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(2);
        env.enableCheckpointing(2000L);

//        env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.244.10:9000/flink/checkpoints"));
//        env.setStateBackend(new RocksDBStateBackend("file:///C:/Users/jaslou/Desktop/checkpoint", true));


        Properties properties = PropertyUtil.getKafkaProperties();

        DataStreamSource<UserBehavior> userBehaviorDataStreamSource = env.addSource(new UserBehaviorSource());
//        userBehaviorDataStreamSource.assignTimestampsAndWatermarks(new UserBehaviorTimeAssigner(Time.seconds(5)))
//                .addSink(new FlinkKafkaProducer011<UserBehavior>(
//                        KAFKA_TOPIC,
//                        new KeyedSerializationSchemaWrapper(new UserBehaviorSerializationSchema()),
//                        properties,
//                        Optional.empty(),
//                        FlinkKafkaProducer011.Semantic.EXACTLY_ONCE,
//                        5
//                        )
//                );
        userBehaviorDataStreamSource
                .assignTimestampsAndWatermarks(new UserBehaviorTimeAssigner(Time.seconds(5)))
                .addSink(new FlinkKafkaProducer011<UserBehavior>(
                                KAFKA_TOPIC,
                                new KeyedSerializationSchemaWrapper(new UserBehaviorSerializationSchema()),
                                properties,
                                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
                        )
                );
        // topic动态发现
//        Pattern pattern =Pattern.compile("flink_topic[0-9]");

        env.execute("The job with sending data to kafka Job ");
    }

}

