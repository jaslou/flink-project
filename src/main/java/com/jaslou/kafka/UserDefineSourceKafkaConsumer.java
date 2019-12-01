package com.jaslou.kafka;

import com.jaslou.domin.UserBehavior;
import com.jaslou.domin.UserBehaviorSerializationSchema;
import com.jaslou.util.PropertyUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Optional;
import java.util.Properties;

/**
 * receive data from kafka
 */
public class UserDefineSourceKafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        Properties properties = PropertyUtil.getKafkaProperties();
        DataStreamSource<UserBehavior> userBehaviorDataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "flink_topic",
                new UserBehaviorSerializationSchema(),
                properties
        ));

        userBehaviorDataStreamSource.print();

        env.execute("The job with  receiving data from kafka ");

    }
}
