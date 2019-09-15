package com.jaslou.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MyListState extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    private transient ListState<String> myState;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("listState", String.class);
        myState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {

        List<String> currentList = new ArrayList<>();

        if ("flink".equals(input.f1)){
            currentList.add(input.f1);
        }

        myState.update(currentList);

        Iterable<String> currentState = myState.get();

        for (String s : currentState) {
            out.collect(new Tuple2<>(input.f0, s));
        }

//        currentState.forEach(s ->  out.collect(new Tuple2<>(input.f0, s)));

    }
}
