package com.jaslou.marketAnalysis;

import com.jaslou.marketAnalysis.domain.AppMarketUserBehavior;
import com.jaslou.marketAnalysis.domain.MarketViewCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * 根据渠道和用户行为，统计用户使用APP情况
 */
public class MarketAnalysisChannelMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.addSource(new MarketUserbehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AppMarketUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(AppMarketUserBehavior element) {
                        return element.timestamp;
                    }
                })
                .filter(data -> !data.behavior.equals("uninstall"))
                .map(new MyMapFunction())
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1), Time.seconds(10))
                .process(new MarketAnalysisCountByChannel())
                .print()
                ;

        env.execute("Market analysis job");
    }
}

// 自定义处理
class MarketAnalysisCountByChannel extends ProcessWindowFunction<Tuple2<String, Long>, MarketViewCount, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<MarketViewCount> out) throws Exception {
        String windowStart = new Timestamp(context.window().getStart()).toString();
        String windowEnd = new Timestamp(context.window().getEnd()).toString();
        String channel = key.split("_")[0];
        String behavior = key.split("_")[1];
        Long count = 0L;
        Iterator<Tuple2<String, Long>> iterator = elements.iterator(); // elements 该窗口的所有所有元素
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        out.collect(new MarketViewCount(windowStart, windowEnd, channel, behavior, count));
    }
}

// Map实现类
class MyMapFunction implements MapFunction<AppMarketUserBehavior, Tuple2<String, Long>> {
    @Override
    public Tuple2<String, Long> map(AppMarketUserBehavior value) throws Exception {
        return new Tuple2<>(value.channel + "_" + value.behavior, 1L);
    }
}
