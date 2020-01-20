package com.jaslou.loginAnalysis;

import com.jaslou.loginAnalysis.domain.LoginEvent;
import com.jaslou.loginAnalysis.domain.LoginWarningMessage;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginAnalysiMain {
    public static void main(String[] args) throws Exception {
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

        SingleOutputStreamOperator<LoginWarningMessage> process = env.createInput(pojoCsvInputFormat, typeInfo)
                .keyBy(r -> r.userId)
                .process(new LoginKeyProcessFunction(3));
        process.print();
        env.execute("login fail message");
    }
}

class LoginKeyProcessFunction extends KeyedProcessFunction<Long, LoginEvent, LoginWarningMessage> {

    int times;

    ListState<LoginEvent> loginEventListState;

    public LoginKeyProcessFunction(int times) {
        this.times = times;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginWarningMessage> out) throws Exception {
        List<LoginEvent> loginEventList = new ArrayList<>();
        Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
        while (iterator.hasNext()) {
            LoginEvent next = iterator.next();
            loginEventList.add(next);
        }
        if (loginEventList.size() >= times) {
            out.collect(new LoginWarningMessage(loginEventList.get(0).userId,
                    loginEventList.get(0).timestamp,
                    loginEventList.get(loginEventList.size() - 1).timestamp,
                    "userId:" + loginEventList.get(0).userId + "\tlogin fail " + loginEventList.size() + " times in 2 second"));
        }
        loginEventListState.clear();
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginWarningMessage> out) throws Exception {
        Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
        if (value.status.equals("fail")) {
            if (!iterator.hasNext()) {
                ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 2000L);
            }
            loginEventListState.add(value);
        } else {
            loginEventListState.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<LoginEvent> loginEventListStateDescriptor = new ListStateDescriptor<>("", LoginEvent.class);
        loginEventListState = getRuntimeContext().getListState(loginEventListStateDescriptor);
    }
}
