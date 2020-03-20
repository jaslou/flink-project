package com.jaslou.watermark;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.mutable.StringBuilder;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class WatermarkMain {

    public static  OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("delay_data") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(2000L);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> processStream = inputStream.map(new MyMapFunction())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    public long currentMaxTimestamp = 0L;
                    public final long maxOutOfOrderness = 2000L;
                    Watermark watermark = null;

                    @Override
                    public Watermark getCurrentWatermark() {
                        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                        long timestamp = element.f1;
                        if (timestamp > currentMaxTimestamp) {
                            currentMaxTimestamp = timestamp;
                        }
                        StringBuilder stringBuilder = new StringBuilder("");
                        stringBuilder.append("ID:" + element.f0).append(",")
                                .append("timestamp:")
                                .append(element.f1 + "|" + new Timestamp(element.f1)).append(",")
                                .append("currentMaxTimestamp:")
                                .append(currentMaxTimestamp + "|" + new Timestamp(currentMaxTimestamp)).append(",")
                                .append("watermark:")
                                .append(watermark.getTimestamp() + "|" + new Timestamp(watermark.getTimestamp()));
                        System.out.println(stringBuilder.toString());
                        return timestamp;
                    }
                })
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .process(new MyPoccessWindowFunction());


        processStream.print("normal data");
        processStream.getSideOutput(outputTag).print("delay data");

        env.execute("watermark job!");
    }
}

class MyMapFunction implements MapFunction<String, Tuple2<String, Long>>{

    @Override
    public Tuple2<String, Long> map(String value) throws Exception {
        String[] text = value.split(",");
        String ip = text[0];
        Long timestamp = Long.parseLong(text[1]);// time
        /*SimpleDateFormat format = new SimpleDateFormat("dd/mm/yyyy:HH:mm:ss");
        Date date = format.parse(time);
        long timestamp = date.getTime();*/
        return new Tuple2<>(ip, timestamp);
    }
}

class MyPoccessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long count = 0L;
        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
        long firstTimestamp = 0L;
        long endTimestamp = 0L;
        Tuple2<String, Long> next = null;
        while (iterator.hasNext()) {
            next = iterator.next();
            count++;
            if (count == 1) {
                firstTimestamp = next.f1;
            }
            if (!iterator.hasNext()){
                endTimestamp = next.f1;
            }
        }

        StringBuilder stringBuilder = new StringBuilder("");
        stringBuilder.append(s).append(",").append(count).append(",");
        stringBuilder.append("startTimestamp:" + firstTimestamp + "|"
                + new Timestamp(firstTimestamp)
                + ",endTimestamp:" + endTimestamp + "|" + new Timestamp(endTimestamp)).append(",");
        stringBuilder.append("window:" + format.format(context.window().getStart())).append("-")
                .append(format.format(context.window().getEnd()));
        out.collect(stringBuilder.toString());
    }

}


