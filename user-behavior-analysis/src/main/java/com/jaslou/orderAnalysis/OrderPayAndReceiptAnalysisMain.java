package com.jaslou.orderAnalysis;

import com.jaslou.orderAnalysis.domain.OrderEvent;
import com.jaslou.orderAnalysis.domain.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.URL;

/**
 * 使用CoProcessFunction实现双流（支付订单流和支付回值流）对账
 */
public class OrderPayAndReceiptAnalysisMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 获取订单输入流
        SingleOutputStreamOperator<OrderEvent> payStream = getPayStream(env);
        // 设置watermark
        KeyedStream<OrderEvent, String> payKeyedStream = payStream
                .filter(r -> r.behavior.equals("pay")) // 只选择支付行为的订单记录
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.timestamp * 1000L;
                    }
                }).keyBy(r -> r.txId);

        //获取支付回执输入流
        SingleOutputStreamOperator<ReceiptEvent> receiptStream = getReceiptStream(env);
        // 设置watermark
        KeyedStream<ReceiptEvent, String> receiptKeyedStream = receiptStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.timestamp * 1000L;
            }
        }).keyBy(r -> r.txId);

        // 定义侧输出流tag
        OutputTag<OrderEvent> orderEventOutputTag = new OutputTag<OrderEvent>("pay-tag"){};
        OutputTag<ReceiptEvent> receiptEventOutputTag = new OutputTag<ReceiptEvent>("receipt-tag"){};

        // 连接两条流，共同处理
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> process = payKeyedStream.connect(receiptKeyedStream).process(new MyCoProcessFunction(orderEventOutputTag, receiptEventOutputTag));


        process.print("matched");
        process.getSideOutput(orderEventOutputTag).print("unmatched receipt");
        process.getSideOutput(receiptEventOutputTag).print("unmatched pay");

    }

    // 获取支付订单输入流
    public static  SingleOutputStreamOperator<OrderEvent> getPayStream(StreamExecutionEnvironment env) throws Exception {
        // 读取文件
        URL resource = OrderPayAnalysisMain.class.getClassLoader().getResource("OrderLog.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));

        // 定义字段
        String[] fieldNames = {"orderId", "behavior", "txId", "timestamp"};

        // 定义OrderEvent 的TypeInformation
        PojoTypeInfo<OrderEvent> typeInfo = (PojoTypeInfo<OrderEvent>) TypeExtractor.createTypeInfo(OrderEvent.class);
        // 创建PojoCsvInputFormat
        PojoCsvInputFormat<OrderEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);

        // 创建输入流
        SingleOutputStreamOperator<OrderEvent> singleOutputStreamOperator = env.createInput(pojoCsvInputFormat, typeInfo);
        return singleOutputStreamOperator;
    }

    // 获取支付回执输入流
    public static  SingleOutputStreamOperator<ReceiptEvent> getReceiptStream(StreamExecutionEnvironment env) throws Exception {
        // 读取文件
        URL resource = OrderPayAnalysisMain.class.getClassLoader().getResource("receiptLog.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));

        // 定义字段
        String[] fieldNames = {"txId", "payChannel", "timestamp"};

        // 定义OrderEvent 的TypeInformation
        PojoTypeInfo<ReceiptEvent> typeInfo = (PojoTypeInfo<ReceiptEvent>) TypeExtractor.createTypeInfo(ReceiptEvent.class);
        // 创建PojoCsvInputFormat
        PojoCsvInputFormat<ReceiptEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);

        // 创建输入流
        SingleOutputStreamOperator<ReceiptEvent> singleOutputStreamOperator = env.createInput(pojoCsvInputFormat, typeInfo);
        return singleOutputStreamOperator;
    }

}

class MyCoProcessFunction extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

    // 定义状态，保存支付和回执的事件
    private ValueState<OrderEvent> payState = null;
    private ValueState<ReceiptEvent> receiptState = null;
    // 定义测输出流
    private OutputTag<OrderEvent> orderEventOutputTag  ;
    private OutputTag<ReceiptEvent> receiptEventOutputTag ;

    public MyCoProcessFunction(OutputTag<OrderEvent> orderEventOutputTag, OutputTag<ReceiptEvent> receiptEventOutputTag) {
        this.orderEventOutputTag = orderEventOutputTag;
        this.receiptEventOutputTag = receiptEventOutputTag;
    }

    // 订单支付事件处理
    @Override
    public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        ReceiptEvent receiptEvent = receiptState.value();
        if (receiptEvent != null) {
            // 如果已经有回执，在主流输出匹配信息，清空状态
            out.collect(Tuple2.of(value, receiptEvent));
            receiptState.clear();
        } else { // 如果回执还没有到，则将支付信息存入状态payState，并注册定时器等待回执到了触发
            payState.update(value);
            ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L); // 5秒
        }
    }

    // 订单回执事件处理
    @Override
    public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        OrderEvent orderEvent = payState.value();
        if (orderEvent != null) { // 如果有支付事件，则输出匹配信息，情况状态
            out.collect(Tuple2.of(orderEvent, value));
            payState.clear();
        } else { // 如果支付事件还没到，则将回执信息存入状态receiptState，并注册定时器等支付执到了触发
            receiptState.update(value);
            ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 5000L);
        }
    }

    // 触发定时器
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 到时间了，还未收到事件，则报警输出
        if (payState.value() != null) {// 说明回执还没到，侧流输出pay事件
            ctx.output(orderEventOutputTag, payState.value());
        }else if (receiptState.value() != null) {
            ctx.output(receiptEventOutputTag, receiptState.value());
        }
        // 情况状态
        payState.clear();
        receiptState.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
        receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state", ReceiptEvent.class));
    }

}

