package com.jaslou.logAnalysis;

import com.jaslou.logAnalysis.domain.LogInfo;
import com.jaslou.logAnalysis.domain.LogViewCount;
import com.jaslou.webAnalysis.PageViewMain;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;

/**
 * 实时统计每小时热门页面的访问量
 */
public class LogAnalysisMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 使用EventTime 处理
        env.setParallelism(1); //设置并行度

        URL resource = PageViewMain.class.getClassLoader().getResource("loginfo.log");
        env
                .readTextFile(resource.getPath())
                .map(new LogMapFUnction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogInfo>(Time.seconds(1)) {
                    @Override // 使用BoundedOutOfOrdernessTimestampExtractor
                    public long extractTimestamp(LogInfo element) {
                        return element.timestamp;
                    }
                })
                .keyBy(r -> r.accessURL)
                .timeWindow(Time.minutes(60), Time.seconds(5))
//                .allowedLateness(Time.seconds(60)) // 允许数据延时到达60秒
                .aggregate(new ApacheLogAgg(), new ResultWindowFunction())
                .keyBy(r -> r.windowEnd)
                .process(new TopNHotUrls(5))
                .print();
        env.execute("Log analysis job！");
    }
}

// 自定义Map处理函数
class LogMapFUnction implements MapFunction<String, LogInfo> {

    @Override
    public LogInfo map(String value) throws Exception {
        String[] logText = value.split(" ");
        String ip = logText[0];
        String time = logText[3];// time
        SimpleDateFormat format = new SimpleDateFormat("dd/mm/yyyy:HH:mm:ss");
        Date date = format.parse(time);
        long timestamp = date.getTime();
        String method = logText[5];
        String url = logText[6];
        return new LogInfo(ip, timestamp, url, method);
    }
}

// 自定义聚合函数-统计数量
class ApacheLogAgg implements AggregateFunction<LogInfo, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LogInfo value, Long accumulator) {
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

//自定义窗口处理函数
class ResultWindowFunction implements WindowFunction<Long, LogViewCount, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<LogViewCount> out) throws Exception {
        out.collect(new LogViewCount(s, window.getEnd(), input.iterator().next()));
    }
}

// 获取访问量前TopN的URL
class TopNHotUrls extends KeyedProcessFunction<Long, LogViewCount, String> {

    public int topSize;

    private ListState<LogViewCount> logState;

    public TopNHotUrls(int topSize) {
        this.topSize = topSize;
    }

    // 初始化状态
    @Override
    public void open(Configuration parameters) throws Exception {
        logState = getRuntimeContext().getListState(new ListStateDescriptor<LogViewCount>("log-state", TypeInformation.of(LogViewCount.class)));
    }

    // 处理每一条数据（存入状态中），并定义定时器
    @Override
    public void processElement(LogViewCount value, Context ctx, Collector<String> out) throws Exception {
        logState.add(value);
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    // 定义触发器
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        //1. 将状态ListState中的LogViewCount提取出来，并存入集合中，最后清空状态
        ArrayList<LogViewCount> logViewCounts = new ArrayList<>();
        /*for (LogViewCount logViewCount : logState.get()) {
            logViewCounts.add(logViewCount);
        }*/
        Iterator<LogViewCount> iterator = logState.get().iterator();
        while (iterator.hasNext()) {
            logViewCounts.add(iterator.next());
        }

        logState.clear();

        //2. 对集合进行排序，使用Comparator
        logViewCounts.sort(new Comparator<LogViewCount>() {
            @Override
            public int compare(LogViewCount o1, LogViewCount o2) {
                return (int) (o2.viewCount - o1.viewCount);
            }
        });

        //3. 拼接打印topN
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        for (int i = 0; i < topSize && i < logViewCounts.size() ; i++) {
            LogViewCount logViewCount = logViewCounts.get(i);
            result.append("No").append(i).append(": ")
                    .append("AccessURL=").append(logViewCount.getAccessURL()).append("\t")
                    .append("浏览量=").append(logViewCount.getViewCount())
                    .append("\n");
        }
        result.append("====================================\n\n");

        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000);

        out.collect(result.toString());
    }
}
