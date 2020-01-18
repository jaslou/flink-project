package com.jaslou.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jaslou.domin.UserBehavior;
import com.jaslou.source.UserBehaviorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.List;

public class FlinkToElasticSearchMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //create flink data Source
        DataStream<UserBehavior> source = env.addSource(new UserBehaviorSource());

        // filter the data
        DataStream<UserBehavior> filterSource = source.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
//                if("buy".equals(userBehavior.behavior)){
//                    return true;
//                }
                return true;
            }
        });

        // transfer data to json
        DataStream<JSONObject> transferSource = filterSource.map(new MapFunction<UserBehavior, JSONObject>() {
            @Override
            public JSONObject map(UserBehavior userBehavior) throws Exception {
                String jsonString = JSONObject.toJSONString(userBehavior, SerializerFeature.WriteDateUseDateFormat);
                System.out.println("当前正在处理:" + jsonString);
                JSONObject jsonObject = JSONObject.parseObject(jsonString);
                return jsonObject;
            }
        });
         // create a elasticsearch instance
        List<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("192.168.244.10", 9200, "http"));
        httpHost.add(new HttpHost("192.168.244.11", 9201, "http"));
        httpHost.add(new HttpHost("192.168.244.12", 9202, "http"));
        ElasticsearchSink.Builder<JSONObject> esSinkBuilder = new ElasticsearchSink.Builder<JSONObject>(
                httpHost,
                new ElasticsearchSinkFunction<JSONObject>() {
                    @Override
                    public void process(JSONObject jsonObject, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        // index=index_userbehavior, type=type_userbehavior, id=userId
                        requestIndexer.add(Requests.indexRequest().index("index_userbehavior").type("type_userbehavior").source(jsonObject));
                    }
                }
        );
        // set numMaxActions
        esSinkBuilder.setBulkFlushMaxActions(50);
        //sink the esSinkBuilder to flink sink
        transferSource.addSink(esSinkBuilder.build());

        env.execute("excute ElasticSearchDemo");

    }

}
