package com.jaslou.orderAnalysis;

import com.jaslou.orderAnalysis.domain.OrderEvent;
import com.jaslou.orderAnalysis.domain.ReceiptEvent;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;

/**
 * 使用Stream join 进行双流Join
 */
public class OrderPayAndReceiptStreamJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000); // 每2秒生成一次水位线

        // 获取订单输入流
        SingleOutputStreamOperator<OrderEvent> payStream = getPayStream(env);
        // 设置watermark
        KeyedStream<OrderEvent, String> payKeyedStream = payStream
                .filter(r -> r.behavior.equals("pay")) // 只选择支付行为的订单记录
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
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

        // join，具有相同Key的事件间隔Join
        SingleOutputStreamOperator<Tuple2<String, String>> process = payKeyedStream.intervalJoin(receiptKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))// 间隔十秒，  A事件的[timestamp - 5, timestamp + 5]的B都会匹配到
                .process(new OrderPayAndReceiptProcessJoinFunction());

        process.print();

        env.execute("two stream join job");

    }

    // 获取支付回执输入流
    public static SingleOutputStreamOperator<ReceiptEvent> getReceiptStream(StreamExecutionEnvironment env) throws Exception {
        // 读取文件
        URL resource = OrderPayAndReceiptStreamJoin.class.getClassLoader().getResource("joinreceiptLog.csv");
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

    // 获取支付订单输入流
    public static  SingleOutputStreamOperator<OrderEvent> getPayStream(StreamExecutionEnvironment env) throws Exception {
        // 读取文件
        URL resource = OrderPayAndReceiptStreamJoin.class.getClassLoader().getResource("joinOrderLog.csv");
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
}

// 实现双流join接口
class OrderPayAndReceiptProcessJoinFunction extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<String, String>>{

    @Override
    public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        System.out.println(ctx.getTimestamp());
        System.out.println(ctx.getLeftTimestamp() + "-" + ctx.getRightTimestamp());
        out.collect(Tuple2.of(left.toString(), right.toString()));
    }
}
