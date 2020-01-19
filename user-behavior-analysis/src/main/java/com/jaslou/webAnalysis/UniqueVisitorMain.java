package com.jaslou.webAnalysis;

import com.jaslou.webAnalysis.domain.UVCount;
import com.jaslou.webAnalysis.domain.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;

/**
 * 实时统计UV
 */
public class UniqueVisitorMain {
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
                .timeWindowAll(Time.hours(1))
                .apply(new UVWindowFunction())
                .print();
        env.execute("UV Job!");
    }
}

// 统计UV，使用Set进行去重
class UVWindowFunction implements AllWindowFunction<UserBehavior, UVCount, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UVCount> out) throws Exception {
        HashSet<Long> set = new HashSet<Long>();
        Iterator<UserBehavior> iterator = values.iterator();
        while (iterator.hasNext()) {
            set.add(iterator.next().userId);
        }
        out.collect(new UVCount(window.getEnd(), set.size()));
    }
}


