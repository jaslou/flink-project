package com.jaslou.adclickAnalysis;

import com.jaslou.adclickAnalysis.domain.AdClickEvent;
import com.jaslou.adclickAnalysis.domain.AdClickResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;

/**
 * 统计每个省份广告点击量
 */
public class AdClickAnalysisMain {

    public static void main(String[] args) throws Exception {
        // 1. 创建Execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取文件
        URL resource = AdClickAnalysisMain.class.getClassLoader().getResource("AdClickLog.csv");
        Path filePath = Path.fromLocalFile(new File(resource.toURI()));

        // 定义POJO类型TypeInformation（AdClickEvent）
        PojoTypeInfo<AdClickEvent> pojoTypeInfo = (PojoTypeInfo<AdClickEvent>)TypeExtractor.createTypeInfo(AdClickEvent.class);
        // 指定对应的从csv读取文件的字段顺序
        String[] fieldNames = new String[]{"userId","adId","province", "city", "timestamp"};


        // 3. 创建PojoCsvInputFormat对象
        PojoCsvInputFormat<AdClickEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldNames);

        // 创建数据源，AdClickEvent类型的DataStream
        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = env.createInput(pojoCsvInputFormat, pojoTypeInfo)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.timestamp * 1000L;
                    }
                });

        // 4. 使用process function，过滤大量点击用户
        // 定义侧输出流
        OutputTag<String> outputTagAdClick = new OutputTag<>("filterUserClick",TypeExtractor.createTypeInfo(String.class));
        SingleOutputStreamOperator<AdClickEvent> adClickFilterUser = adClickEventStream
                .keyBy("userId", "adId")// KeyStream<AdClickEvent, Tuple>
                .process(new AdClickKeyedProcessFunction(100, outputTagAdClick));

        // 使用Lambada表达式
        /*adClickFilterStream
                .keyBy(r -> r.province)// 返回的是KeyStream<AdClickEvent, String>，如果用keyBy("province")的话返回的是KeyStream<AdClickEvent, Tuple>
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdClickAggregate(), new AdClickWindowFunction())
                .print();*/

        // 5. 按省份分组
        SingleOutputStreamOperator<AdClickResult> provinceAdClickStream = adClickFilterUser
                .keyBy("province")// 返回的是KeyStream<AdClickEvent, String>，如果用keyBy("province")的话返回的是KeyStream<AdClickEvent, Tuple>
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdClickAggregate(), new AdClickWindowFunction());

        //6. 输出结果
        provinceAdClickStream.print("provinceAdClickCount");// 主流输出
        adClickFilterUser.getSideOutput(outputTagAdClick).print("blockUserList"); // 侧流输出

        env.execute("Ad click job");
    }
}

class AdClickAggregate implements AggregateFunction<AdClickEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent value, Long accumulator) {
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

// Key为String
/*class AdClickWindowFunction implements WindowFunction<Long, AdClickResult, String, TimeWindow> {


    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdClickResult> out) throws Exception {
        out.collect(new AdClickResult(new Timestamp(window.getEnd()).toString(), s, null, input.iterator().next()));
    }
}*/

//key 为Tuple
class AdClickWindowFunction implements WindowFunction<Long, AdClickResult, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdClickResult> out) throws Exception {
        String province = ((Tuple1<String>) tuple).f0;
        out.collect(new AdClickResult(new Timestamp(window.getEnd()).toString(), province, null, input.iterator().next()));
    }
}


// 自定义KeyedProcessFunction， 用于过滤同一个adId连续点击超过100次的用户
class AdClickKeyedProcessFunction extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

    // 指定用户点击次数上限
    public int clickMaxCount;

    // 侧输出流
    public OutputTag<String> outputTag;

    // 定义状态，用于保存用户对当前广告的点击
    private ValueState<Long> userClickCount = null;

    // 判断用户是否是黑名单的标志
    public ValueState<Boolean> isFilter = null;

    // 用于保存定时器
    public ValueState<Long> timmer = null;



    public AdClickKeyedProcessFunction(int clickMaxCount, OutputTag<String> outputTag) {
        this.clickMaxCount = clickMaxCount;
        this.outputTag = outputTag;
    }

    // 获取状态
    @Override
    public void open(Configuration parameters) throws Exception {
        userClickCount = getRuntimeContext().getState(new ValueStateDescriptor<>("userClickUser", Types.LONG));
        isFilter = getRuntimeContext().getState(new ValueStateDescriptor<>("isFilter", Boolean.class));
        timmer = getRuntimeContext().getState(new ValueStateDescriptor<>("timmer", Long.class));
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        // 1. 获取当前状态
        Long curCount = userClickCount.value();
        Boolean flag = isFilter.value();

        //2 . 注册定时器，每天凌晨00:00触发
        if (curCount == null) {
            long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
            timmer.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
            curCount = 0L;
        }

        //3. 当前状态和指定上限比较，如果达标，则加入黑名单，并输出以便输出到侧输出流
        if (curCount >= clickMaxCount) {
            if (flag == null) {//如果第一次达到点击上限，则更新标志
                isFilter.update(true);
                // 侧流输出
                ctx.output(outputTag,  "用户：" + value.userId + "\t" + "广告：" + value.adId + "\t 点击了：" + clickMaxCount + "次");
            }
        }else {//4. 否则计数并更新状态
            userClickCount.update(curCount + 1);
            // 主输出流
            out.collect(value);
        }
    }

    // 定时器，定时清楚状态
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        if (timestamp == timmer.value()) {
            userClickCount.clear();
            isFilter.clear();
            timmer.clear();
        }
    }

}
