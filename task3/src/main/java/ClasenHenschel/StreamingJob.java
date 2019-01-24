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

package ClasenHenschel;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        String path = "/Users/oliverclasen/Documents/access_log_Aug95.gz";
        
        DataStream<String> augLog = env.readTextFile(path);
        
        DataStream<Tuple5<String, String, String, Integer, Integer>> orderedAugLog = augLog.flatMap(new FlatMapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple5<String, String, String, Integer, Integer>> collector) throws Exception {
                String client = "-";
                String dateString = "-";
                String request = "-";
                Integer responseCode = 0;
                Integer byteNumber = 0;
                
                Pattern clientPattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher clientMatcher = clientPattern.matcher(s);
                if (clientMatcher.find()) {
                    client = clientMatcher.group();
                }
                
                Pattern datePattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher dateMatcher = datePattern.matcher(s);
                if (dateMatcher.find()) {
                    dateString = dateMatcher.group();
                }
                
                Pattern requestPattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher requestMatcher = requestPattern.matcher(s);
                if (requestMatcher.find()) {
                    request = requestMatcher.group();
                }
                
                Pattern responseCodePattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher responseCodeMatcher = responseCodePattern.matcher(s);
                if (responseCodeMatcher.find()) {
                    responseCode = Integer.parseInt(responseCodeMatcher.group());
                }
                
                Pattern byteNumberPattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher byteNumberMatcher = byteNumberPattern.matcher(s);
                if (byteNumberMatcher.find()) {
                    byteNumber = Integer.parseInt(byteNumberMatcher.group());
                }
                
                collector.collect(new Tuple5<>(client, dateString, request, responseCode, byteNumber));
            }
        });

		/*augLog.filter(line -> {
			Pattern pattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
			Matcher matcher = pattern.matcher(line);
			return matcher.find();})
				.print();*/
        
        
        DataStream<Tuple2<String, Integer>> client = augLog.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Pattern pattern = Pattern.compile("[\\d]{1,3}([\\.][\\d]{1,3}){3}");
                Matcher matcher = pattern.matcher(s);
                if (matcher.find())
                    collector.collect(new Tuple2<>(matcher.group(), 1));
                else
                    collector.collect(new Tuple2<>("test", 1));
            }
        }).keyBy(0).sum(1);
        client.print();
        
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
