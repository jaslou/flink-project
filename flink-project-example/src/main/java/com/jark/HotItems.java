/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jark;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

public class HotItems {

	public static void main(String[] args) throws Exception {

		// 创建 execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 告诉系统按照 EventTime 处理
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
		env.setParallelism(1);
		env.enableCheckpointing(1000);
//		env.setStateBackend(new Rocks)

		// UserBehavior.csv 的本地文件路径, 在 resources 目录下
		URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
		Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
		// 抽取 UserBehavior 的 TypeInformation，是一个 TypeInfo
		PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		// 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
		String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
		// 创建 PojoCsvInputFormat
		PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);


		env
			// 创建数据源，得到 UserBehavior 类型的 DataStream
			.createInput(csvInput, pojoType)
			// 抽取出时间和生成 watermark
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
				@Override
				public long extractAscendingTimestamp(UserBehavior userBehavior) {
					// 原始数据单位秒，将其转成毫秒
					return userBehavior.timestamp * 1000;
				}
			})
			// 过滤出只有点击的数据
			.filter(new FilterFunction<UserBehavior>() {
				@Override
				public boolean filter(UserBehavior userBehavior) throws Exception {
					// 过滤出只有点击的数据
					return userBehavior.behavior.equals("pv");
				}
			})
			.keyBy("itemId")
			.timeWindow(Time.minutes(10), Time.seconds(5))
			.aggregate(new CountAgg(), new ResultWindowFunction())
			.keyBy("windowEnd")
			.process(new TopNHotItems(3))
			.print();

		env.execute("Hot Items Job");
	}

}
