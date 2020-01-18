package com.jaslou.mysql;

import com.google.common.collect.Lists;
import com.jaslou.domin.UserBehavior;
import com.jaslou.domin.UserBehaviorSerializationSchema;
import com.jaslou.util.PropertyUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkSinkToMysqlMain  {



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);

        Properties properties = PropertyUtil.getKafkaProperties();
        DataStreamSource<UserBehavior> userBehaviorDataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "flink_topic",
                new UserBehaviorSerializationSchema(),
                properties
        ));

        userBehaviorDataStreamSource.timeWindowAll(Time.seconds(30)).apply(new AllWindowFunction<UserBehavior, List<UserBehavior>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<List<UserBehavior>> out) throws Exception {
                ArrayList<UserBehavior> list = Lists.newArrayList(values);
                if(list.size() > 0){
                    System.out.println("30秒内收集到 UserBehavior 的数据条数是：" + list.size());
                    out.collect(list);
                }
                out.collect(list);
            }
        }).addSink(new MysqlRichSinkFunction());

        env.execute("mysql sink");

    }

}
