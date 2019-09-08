package com.jaslou.state.java;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 5L), Tuple2.of(1L, 2L), Tuple2.of(1L, 4L))
                .keyBy(0)
                .flatMap(new CountWindowKeyedState())
                .print();
        env.execute("key state");
        // the printed output will be (1,4) 、 (1,5) and (1,6)
    }
}
