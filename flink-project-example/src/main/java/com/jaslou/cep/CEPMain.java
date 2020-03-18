package com.jaslou.cep;

import com.jaslou.assigner.SensorTimeAssigner;
import com.jaslou.source.SensorEvent;
import com.jaslou.source.SensorSource;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * 警告: 传感器10秒内连续2次读数超过阈值
 * 报警：20秒内连续匹配到警告
 */
public class CEPMain {

    public final static long MAX_TEMPERATURE = 98;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取文件
        String[] fieldNames = {"id","timestamp","temperature"};
        URL resource = CEPMain.class.getClassLoader().getResource("sensorreading.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));
        PojoTypeInfo<SensorEvent> typeInfo = (PojoTypeInfo<SensorEvent>) TypeExtractor.createTypeInfo(SensorEvent.class);
        PojoCsvInputFormat<SensorEvent> csvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);

        // 创建输入流
        SingleOutputStreamOperator<SensorEvent> inputEventStream = env.createInput(csvInputFormat, typeInfo)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorEvent>(Time.seconds(1L)) {
                    @Override
                    public long extractTimestamp(SensorEvent element) {
                        return element.timestamp;
                    }
                });

        // create input stream
        /*SingleOutputStreamOperator<SensorEvent> inputEventStream = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());*/

        // 创建警告模式
        Pattern<SensorEvent, SensorEvent> warningPattern = Pattern.<SensorEvent>begin("first_event")
                .where(new SimpleCondition<SensorEvent>() {
                    @Override
                    public boolean filter(SensorEvent value) throws Exception {
                        return value.temperature >= MAX_TEMPERATURE;
                    }
                }).next("second_event")
                .where(new IterativeCondition<SensorEvent>() {
                    @Override
                    public boolean filter(SensorEvent value, Context<SensorEvent> ctx) throws Exception {
                        return value.temperature >= MAX_TEMPERATURE;
                    }
                }).within(Time.seconds(10));

        // 创建警告模式流
        PatternStream<SensorEvent> warningPatternStream = CEP.pattern(inputEventStream.keyBy("id"), warningPattern);

        //  创建警告流
        SingleOutputStreamOperator<SensorWarning> warningStream = warningPatternStream.flatSelect(new PatternFlatSelectFunction<SensorEvent, SensorWarning>() {
            @Override
            public void flatSelect(Map<String, List<SensorEvent>> pattern, Collector<SensorWarning> out) throws Exception {
                SensorEvent first_event = pattern.get("first_event").get(0);
                SensorEvent second_event = pattern.get("second_event").get(0);
                out.collect(new SensorWarning(first_event.id,
                        (first_event.temperature + second_event.temperature) / 2));

            }
        });

        // 创建报警模式
        Pattern<SensorWarning, SensorWarning> alertPattern = Pattern.<SensorWarning>begin("first_warning")
                .next("second_warning").times(4).within(Time.seconds(20));
        // 创建报警模式流
        PatternStream<SensorWarning> alertPatternStream = CEP.pattern(warningStream.keyBy("id"), alertPattern);

        // 创建报警流
        SingleOutputStreamOperator<SensorAlert> alertStream = alertPatternStream.flatSelect(new PatternFlatSelectFunction<SensorWarning, SensorAlert>() {
            @Override
            public void flatSelect(Map<String, List<SensorWarning>> pattern, Collector<SensorAlert> out) throws Exception {
                SensorWarning first_warning = pattern.get("first_warning").get(0);
                SensorWarning second_warning = pattern.get("second_warning").get(0);
                if (first_warning.averageTemperature <= second_warning.averageTemperature) {
                    out.collect(new SensorAlert("#############################_" + first_warning.id));
                }
            }
        });
        // 打印输出
        alertStream.print();

        env.execute("cep Job");

    }
}
