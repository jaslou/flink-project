package com.jaslou.hbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class HBaseRichSinkFunction extends RichSinkFunction {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {

    }
}
