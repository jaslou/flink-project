package com.jaslou.orderAnalysis;

import com.jaslou.orderAnalysis.domain.OrderEvent;
import com.jaslou.orderAnalysis.domain.OrderTimeResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderPayAnalysisMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // 读取文件
        URL resource = OrderPayAnalysisMain.class.getClassLoader().getResource("OrderLog.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));

        // 定义字段
        String[] fieldNames = {"orderId", "behavior", "txId", "timestamp"};

        // 定义OrderEvent 的TypeInformation
        PojoTypeInfo<OrderEvent> typeInfo = (PojoTypeInfo<OrderEvent>)TypeExtractor.createTypeInfo(OrderEvent.class);
        // 创建PojoCsvInputFormat
        PojoCsvInputFormat<OrderEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);

        // 创建输入流
        KeyedStream<OrderEvent, Long> keyedStream = env.createInput(pojoCsvInputFormat, typeInfo)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.timestamp * 1000L;
                    }
                })
                .keyBy(r -> r.orderId);

        // 定义模式匹配：下单后15分钟内需要支付，否则侧流输出预警
        Pattern<OrderEvent, OrderEvent> eventPattern = Pattern.<OrderEvent>begin("begin").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.behavior.equals("create");
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return value.behavior.equals("pay");
            }
        }).within(Time.minutes(15));

        // 使用模式
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, eventPattern);

        // 定义侧流OutpuTag
        OutputTag<OrderTimeResult> outputTag = new OutputTag<OrderTimeResult>("timeOutOrderTag") {};


        // 调用select方法，传入侧流OutputTag和正常的PatternSelectFunction以及超时PatternTimeoutFunction
        SingleOutputStreamOperator streamOperator = patternStream.select(outputTag, new OrderPatternTimeoutFunction(), new OrderPatternSelectFunction());

        // 侧流输出结果
        streamOperator.getSideOutput(outputTag).print();

        //主流输出
        streamOperator.print();

        env.execute("order analysis job!");


    }
}

// 自定义超时（15分钟内没有支付的）处理函数
class OrderPatternTimeoutFunction implements PatternTimeoutFunction<OrderEvent, OrderTimeResult> {
    @Override
    public OrderTimeResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
        OrderEvent follow = pattern.get("begin").iterator().next();
        return new OrderTimeResult(follow.orderId, "################################### time out !  ###################################");
    }
}

//自定义正常Order处理函数
class OrderPatternSelectFunction implements PatternSelectFunction<OrderEvent, OrderTimeResult> {
    @Override
    public OrderTimeResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        OrderEvent begin = pattern.get("follow").iterator().next();
        return new OrderTimeResult(begin.orderId, "pay successfully !");
    }
}
