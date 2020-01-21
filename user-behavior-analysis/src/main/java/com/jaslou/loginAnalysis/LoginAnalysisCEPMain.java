package com.jaslou.loginAnalysis;

import com.jaslou.loginAnalysis.domain.LoginEvent;
import com.jaslou.loginAnalysis.domain.LoginWarningMessage;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * 使用CEP实现在2秒内连续两次登录失败的预警
 */
public class LoginAnalysisCEPMain {
    public static void main(String[] args)  throws Exception{
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure the parallelism
        env.setParallelism(1);

        //use the event time for application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginAnalysiMain.class.getClassLoader().getResource("loginrecord.csv");
        Path path = Path.fromLocalFile(new File(resource.toURI()));
        String[] fieldName = {"timestamp","userId", "ipAddress", "status"};
        PojoTypeInfo<LoginEvent> typeInfo = (PojoTypeInfo<LoginEvent>) TypeExtractor.createTypeInfo(LoginEvent.class);
        PojoCsvInputFormat<LoginEvent> pojoCsvInputFormat = new PojoCsvInputFormat<>(path, "\n", "\t", typeInfo, fieldName);

        KeyedStream<LoginEvent, Long> keyedStream = env.createInput(pojoCsvInputFormat, typeInfo)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.timestamp * 1000L;
                    }
                })
                .keyBy(r -> r.userId);

        //定义CEP匹配模式，在2秒内连续两次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.status.equals("fail");
            }
        }).next("middle").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.status.equals("fail");
            }
        }).within(Time.seconds(2));

        //  输入流应用模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        // 实现模式获取函数PatternSelectFunction，检出匹配事件
        SingleOutputStreamOperator<LoginWarningMessage> selectStream = patternStream.select(new LoginFailPatternSelectFunction());

        selectStream.print("pattern stream");

        env.execute("login fail message by cep");
    }
}

// 自定义模式匹配收取类
class LoginFailPatternSelectFunction implements PatternSelectFunction<LoginEvent, LoginWarningMessage> {
    @Override
    public LoginWarningMessage select(Map<String, List<LoginEvent>> pattern) throws Exception {
        LoginEvent begin = pattern.get("begin").iterator().next();
        LoginEvent midlle = pattern.get("middle").iterator().next();
        return new LoginWarningMessage(begin.userId, begin.timestamp, midlle.timestamp, "login fialed !");
    }
}
