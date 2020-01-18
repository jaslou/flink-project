package com.jaslou.hbase;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOutputFormat implements OutputFormat<String> {

    private org.apache.hadoop.conf.Configuration conf = null;
    private HTable table = null;
    private String taskNumber = null;
    private int rowNumber = 0;

    @Override
    public void configure(Configuration parameters) {
        conf = HBaseConfiguration.create();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        table = new HTable(conf, "flinkExample");
        this.taskNumber = String.valueOf(taskNumber);
    }

    @Override
    public void writeRecord(String record) throws IOException {
        Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
        put.add(Bytes.toBytes("entry"), Bytes.toBytes("entry"), Bytes.toBytes(rowNumber));
        rowNumber++;
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        table.flushCommits();
        table.close();
    }
}
