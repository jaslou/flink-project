package com.jaslou.adclickAnalysis;

import com.jaslou.adclickAnalysis.domain.AdClickEvent;
import com.jaslou.adclickAnalysis.domain.AdClickUserCountResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

// count of userId click adID
public class AdClickUserAnalysisMain {
    public static void main(String[] args) throws Exception{
        //1. set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure the parallism
        env.setParallelism(1);
        // use the envent time for application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. read data from csv
        URL resource = AdClickUserAnalysisMain.class.getClassLoader().getResource("AdClickLog.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));

        //3.create a PojoTypeInfo and PojoCsvInputFormat
        PojoTypeInfo<AdClickEvent> typeInfo = (PojoTypeInfo<AdClickEvent>) TypeExtractor.createTypeInfo(AdClickEvent.class);
        // locate the input csv field to POJO
        String[] fieldNames = {"userId","adId","province", "city", "timestamp"};
        PojoCsvInputFormat<AdClickEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);

        //4. create data stream source
        DataStreamSource<AdClickEvent> inputStream = env.createInput(pojoCsvInputFormat, typeInfo);

        // assign timestamp and watermark which are require for event time
        SingleOutputStreamOperator<AdClickEvent> streamOperator = inputStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.timestamp * 1000;
            }
        });

        //5. group AdClickEnvent by userId and adId
        streamOperator
                .keyBy("userId", "adId")
                .timeWindow(Time.hours(1), Time.seconds(30))
                .aggregate(new AdClickUserAndAdAgg(), new AdCLickUserAndAdWindowFunction())
                .keyBy(r -> r.windowEnd)
                .process(new AdClickProcessFunction(3))
                .print();//7. print the result

        //8. execute the job
        env.execute("adClick user job");
    }
}

// compute the count by user-defined aggregate function
class AdClickUserAndAdAgg implements AggregateFunction<AdClickEvent, Long, Long> {
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

class AdCLickUserAndAdWindowFunction implements WindowFunction<Long, AdClickUserCountResult, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdClickUserCountResult> out) throws Exception {
        Long userId = ((Tuple2<Long, Long>) tuple).f0;
        Long adId = ((Tuple2<Long, Long>) tuple).f1;
        /*StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("timeWindow:").append(new Timestamp(window.getStart()))
                .append("-").append(new Timestamp(window.getEnd()))
                .append("\tuserId:").append(userId)
                .append("\tAdId:").append(adId)
                .append("\tcount=").append(input.iterator().next());*/
        out.collect(new AdClickUserCountResult(window.getEnd(), userId, adId, input.iterator().next()));
    }
}

class AdClickProcessFunction extends KeyedProcessFunction<Long, AdClickUserCountResult, String> {
    int topNsize;
    private ListState<AdClickUserCountResult> userResult = null;

    public AdClickProcessFunction(int topNsize) {
        this.topNsize = topNsize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        userResult = getRuntimeContext().getListState(new ListStateDescriptor<>("userListState", AdClickUserCountResult.class));
    }

    @Override
    public void processElement(AdClickUserCountResult value, Context ctx, Collector<String> out) throws Exception {
        userResult.add(value);//
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<AdClickUserCountResult> resultList = new ArrayList<>();
        Iterator<AdClickUserCountResult> iterator = userResult.get().iterator();
        while (iterator.hasNext()) {
            resultList.add(iterator.next());
        }

        userResult.clear();

        resultList.sort(new Comparator<AdClickUserCountResult>() {
            @Override
            public int compare(AdClickUserCountResult o1, AdClickUserCountResult o2) {
                return (int) (o2.count - o1.count);
            }
        });

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("==============================\n");
        stringBuilder.append("Time:" + new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < resultList.size() && i <= topNsize; i++) {
            AdClickUserCountResult adClick = resultList.get(i);
            stringBuilder.append("userId:").append(adClick.userId).append("\t");
            stringBuilder.append("adId:").append(adClick.adId).append("\t");
            stringBuilder.append("count:").append(adClick.count).append("\n");
        }
        Thread.sleep(1000L);
        out.collect(stringBuilder.toString());
    }


}
