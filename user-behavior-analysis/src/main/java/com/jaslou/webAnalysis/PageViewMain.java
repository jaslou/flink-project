package com.jaslou.webAnalysis;

import com.jaslou.webAnalysis.domain.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

/**
 * 实时统计PV：类似与统计word count
 */
public class PageViewMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = PageViewMain.class.getClassLoader().getResource("UserBehavior.csv");
        env
                .readTextFile(resource.getPath())
                .map(new PVMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.timestamp * 1000;
                    }
                })
                .filter(data -> data.behavior.equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2("pv",1L);
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .sum(1)
                .print();

        env.execute("PV Job!");
    }
}

class PVMapFunction implements MapFunction<String, UserBehavior> {
    @Override
    public UserBehavior map(String value) throws Exception {
        String[] user = value.split(",");
        long userId = Long.parseLong(user[0]);
        long itemId = Long.parseLong(user[1]);
        int categoryId = Integer.parseInt(user[2]);
        String behavior = user[3];
        Long timestamp = Long.parseLong(user[4]);
        return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
    }
}


