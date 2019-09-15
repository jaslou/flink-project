package com.jaslou.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 *Example for ValueState
 */
public class CountWindowKeyedState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentState = sum.value();

        //update the count
        currentState.f0 += 1;

        // add the second field of the input value
        currentState.f1 += input.f1;

        sum.update(currentState);

        if(currentState.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentState.f1 / currentState.f0));
            sum.clear();
        }

    }

    @Override
    public void open(Configuration parameters)  {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),
                Tuple2.of(0L, 0L)
        );
        sum = getRuntimeContext().getState(descriptor);
    }
}
