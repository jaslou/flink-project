package com.jaslou.state.java;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        Tuple2<Long, Long> currentValue = sum.value();

        currentValue.f0 = input.f0;

//        currentValue.f1 += input.f1;

        if (currentValue.f1 < input.f1){
            currentValue.f1 = input.f1;
            sum.update(currentValue);
        }

        out.collect(new Tuple2<>(currentValue.f0, currentValue.f1));

    }

    @Override
    public void open(Configuration parameters)  {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "state",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),
                Tuple2.of(0L, 0L)
                );
        sum = getRuntimeContext().getState(descriptor);
    }
}
