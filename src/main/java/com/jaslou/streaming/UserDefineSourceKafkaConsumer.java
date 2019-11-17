package com.jaslou.streaming;

import com.jaslou.domin.UserBehavior;
import com.jaslou.domin.UserDeserializationSchema;
import com.jaslou.source.UserBehaviorSource;
import com.jaslou.util.PropertyUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

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
        DataStreamSource<UserBehavior> userBehaviorDataStreamSource = env.addSource(new FlinkKafkaConsumer011<UserBehavior>(
                "flink_topic",
                new UserDeserializationSchema(),
                properties
        ));

        userBehaviorDataStreamSource.print();

        env.execute("The job with  receiving data from kafka ");

    }
}
