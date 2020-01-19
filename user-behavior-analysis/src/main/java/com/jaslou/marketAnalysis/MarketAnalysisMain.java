package com.jaslou.marketAnalysis;

import com.jaslou.marketAnalysis.domain.AppMarketUserBehavior;
import com.jaslou.marketAnalysis.domain.MarketViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.StreamTokenizer;
import java.sql.Timestamp;

/**
 * 统计用户使用APP情况
 */
public class MarketAnalysisMain {
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
                .filter(new FilterFunction<AppMarketUserBehavior>() {
                    @Override
                    public boolean filter(AppMarketUserBehavior value) throws Exception {
                        return !value.behavior.equals("uninstall");
                    }
                })
                .map(new MapFunction<AppMarketUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(AppMarketUserBehavior value) throws Exception {
                        return new Tuple2<>("appKey", 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1), Time.seconds(10))
                .aggregate(new MarketAnalysisAgg(), new ResultWindowFunction())
                .print()
        ;

        env.execute("Market analysis job");
    }
}

class MarketAnalysisAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

// 自定义窗口函数
class ResultWindowFunction implements WindowFunction<Long, MarketViewCount, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<MarketViewCount> out) throws Exception {
        String windowStart = new Timestamp(window.getStart()).toString();
        String windowEnd = new Timestamp(window.getEnd()).toString();
        Long count = input.iterator().next();
        out.collect(new MarketViewCount(windowStart, windowEnd, count));
    }
}

