package com.jaslou.webAnalysis;

import com.jaslou.webAnalysis.domain.UVCount;
import com.jaslou.webAnalysis.domain.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

/**
 * 使用布隆过滤器统计UV
 */
public class UniqueVisitorBloomMain {

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
                        return new Tuple2<>("SetKey", value.userId);
                    }
                })
                        .keyBy(r -> r.f0)
                        .timeWindow(Time.hours(1))
                        .trigger(new UniqueVisitorTrigger())
                        .process(new UVProcessWindowFunction())
                        .print()
                ;
        env.execute("Count UV with bloom");
    }
}

/**
 * 自定义窗口触发器
 * 窗口每来一条数据就会触发
 */
class UniqueVisitorTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {

    @Override
    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;// 每来一条数据就触发窗口，并清楚状态
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}

// 自定义窗口处理函数，使用布隆过滤器来统计uv，相当于去重
class UVProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, UVCount, String, TimeWindow> {

    private static long cap = 2<<29;// 初始化大小
    private static int seed = 61;//初始化因子

    private Jedis jedis = null;
    private BloomFilter bloom  = null;

    // 创建redis连接
    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("127.0.0.1", 6379);
        jedis.auth("123456");
        bloom  = new BloomFilter(cap, seed);
    }

    @Override
    public void process(String s, Context context, java.lang.Iterable<Tuple2<String, Long>> elements, Collector<UVCount> out) throws Exception {
        // key:windowEnd, value:bitmap
        Long windowEnd = context.window().getEnd();
        String key = String.valueOf(windowEnd);
        int count = 0;
        // redis 表uvcount中存windowEnd->uvcount，每个窗口的uv的count
        String uvcount = jedis.hget("count", key);
        if (uvcount != null) {
            count = Integer.parseInt(uvcount);
        }
        // 判断《当前用户》是否存在，使用布隆过滤器，并获取hash值
        String userID = elements.iterator().next().f1.toString();
        Long offset = bloom.hash(userID);
        // 从redis中获取标识位
        Boolean getbit = jedis.getbit(key, offset);
        if (!getbit) { // 如果不存在，位图对应位置置1，并统计uv数量
            jedis.setbit(key, offset, true);
            jedis.hset("count", key, String.valueOf(count + 1));
//            out.collect(new UVCount(windowEnd, (count + 1)));
        } else { // 如果存在该用户，则不做操作
//            out.collect(new UVCount(windowEnd, count ));
        }
    }
}
