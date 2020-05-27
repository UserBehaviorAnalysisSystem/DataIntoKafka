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

package cn;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import cn.UserBehavior;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "flink-group");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		//String path = "D:\\学习\\毕设\\DataIntoKafka\\src\\main\\resources\\UserBehaviorSmall.csv";
		//String path = "D:\\学习\\毕设\\DataIntoKafka\\src\\main\\resources\\UserBehavior.csv";
		String path = "D:\\学习\\毕设\\Project\\src\\main\\resources\\final\\rawData.csv";
		env.addSource(new SourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception{
				BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path)));
				while(true){
					String line = bufferedReader.readLine();
					if(line != null){
						ctx.collect(line);
					}else{
						System.out.println("empty");
					}
					Thread.sleep(100);
				}
			}
			@Override
			public void cancel() {

			}
		})
			.map(new MapFunction<String, UserBehavior>() {
				@Override
				public UserBehavior map(String s) throws Exception{
					UserBehavior ret = UserBehavior.parseString(s);
					return ret;
				}
			})
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
				@Override
				public long extractAscendingTimestamp(UserBehavior userBehavior) {
					// 原始数据单位秒，将其转成毫秒
					return userBehavior.getTimestamp() * 1000;
				}
			})
			.map(new MapFunction<UserBehavior, String>() {
				@Override
				public String map(UserBehavior u){
					String ret = JSONObject.toJSONString(u);
					System.out.println(ret);
					return ret;
				}
			})
			.addSink(new FlinkKafkaProducer<String>(
					"data",
					new SimpleStringSchema(),
					props
			));

		env.addSource(new SourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception{
				BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path)));
				while(true){
					String line = bufferedReader.readLine();
					if(line != null){
						ctx.collect(line);
					}else{
						System.out.println("empty");
					}
					Thread.sleep(99);
				}
			}
			@Override
			public void cancel() {

			}
		})
			.map(new MapFunction<String, UserBehavior>() {
				@Override
				public UserBehavior map(String s) throws Exception{
					UserBehavior ret = UserBehavior.parseString(s);
					return ret;
				}
			})
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
				@Override
				public long extractAscendingTimestamp(UserBehavior userBehavior) {
					// 原始数据单位秒，将其转成毫秒
					return userBehavior.getTimestamp() * 1000;
				}
			})
			.map(new MapFunction<UserBehavior, String>() {
				@Override
				public String map(UserBehavior u){
					String ret = JSONObject.toJSONString(u);
					System.out.println(ret);
					return ret;
				}
			})
			.addSink(new FlinkKafkaProducer<String>(
					"topN",
					new SimpleStringSchema(),
					props
			));
				/*.addSink(new RichSinkFunction<String>() {
					@Override
					public void invoke(String s, Context context) throws Exception {
						ProducerRecord<String, String> record = new ProducerRecord<>("data", s);
						ProducerRecord<String, String> record1 = new ProducerRecord<>("topN", s);

						producer.send(record);
						producer.send(record1);
					}
				});*/

		env.execute("data into kafka");
		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
	}
}
