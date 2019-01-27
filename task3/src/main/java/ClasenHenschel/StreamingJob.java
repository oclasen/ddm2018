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

import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

public class StreamingJob {
    
    public static void main(String[] args) throws Exception {
        
        String path = "NASA_access_log_Jul95.gz";
        String cores = "4";
        
        //since the jar does not work, we mocked the input
        String[] mockedArgs = {"--path", path, "--cores", cores};
        
        String client_path = "ClasenHenschelMaxClient";
        String request_path = "ClasenHenschelMaxRequest";
        String resource_path = "ClasenHenschelLargestResource";
        
        File clientFile = new File(client_path);
        if (clientFile.exists()) {
            clientFile.delete();
        }
        File requestFile = new File(request_path);
        if (requestFile.exists()) {
            requestFile.delete();
        }
        File resourceFile = new File(resource_path);
        if (resourceFile.exists()) {
            resourceFile.delete();
        }
        
        Options options = new Options();
        
        Option filePath = new Option("p", "path", true, "input file path");
        filePath.setRequired(false);
        options.addOption(filePath);
        
        Option coresOption = new Option("c", "cores", true, "number of cores");
        coresOption.setRequired(false);
        options.addOption(coresOption);
        
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        //parsing the mocked input
        try {
            cmd = parser.parse(options, mockedArgs);
            if (cmd.hasOption("path")) {
                String newPath = cmd.getOptionValue("path");
                File testFile = new File(newPath);
                if (testFile.exists()) {
                    path = newPath;
                }
            }
            
            if (cmd.hasOption("cores")) {
                String newCores = cmd.getOptionValue("cores");
                cores = newCores;
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            
            System.exit(1);
        }
        
        System.out.println("Starting analyse of log: " + path + "\n");
        
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<String> augLog = env.readTextFile(path);
        
        DataStream<Tuple5<String, String, String, Integer, Integer>> orderedAugLog = augLog.flatMap(new FlatMapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple5<String, String, String, Integer, Integer>> collector) throws Exception {
                String client = "-";
                String dateString = "-";
                String request = "-";
                Integer responseCode = 0;
                Integer byteNumber = 0;
                
                //TODO: Regex for the 5 columns
                
                Pattern clientPattern = Pattern.compile("[\\w]+[[\\.-][\\w]+]*");
                Matcher clientMatcher = clientPattern.matcher(s);
                if (clientMatcher.find()) {
                    client = clientMatcher.group();
                }
                
                Pattern datePattern = Pattern.compile("\\[[\\w\\/: -]+\\]");
                Matcher dateMatcher = datePattern.matcher(s);
                if (dateMatcher.find()) {
                    dateString = dateMatcher.group();
                }
                
                Pattern requestPattern = Pattern.compile("\"[\\w\\.\\/,\\? -]+\"");
                Matcher requestMatcher = requestPattern.matcher(s);
                if (requestMatcher.find()) {
                    request = requestMatcher.group();
                }
                
                Pattern responseCodePattern = Pattern.compile("\\s([\\d]{1,4})\\s");
                Matcher responseCodeMatcher = responseCodePattern.matcher(s);
                if (responseCodeMatcher.find()) {
                    responseCode = Integer.parseInt(responseCodeMatcher.group().replaceAll(" ", ""));
                }
                
                Pattern byteNumberPattern = Pattern.compile("\\d+\\s(\\d+)");
                Matcher byteNumberMatcher = byteNumberPattern.matcher(s);
                if (byteNumberMatcher.find()) {
                    byteNumber = Integer.parseInt(byteNumberMatcher.group(1).replaceAll(" ", ""));
                }
                
                collector.collect(new Tuple5<>(client, dateString, request, responseCode, byteNumber));
            }
        });
        System.out.println("Analysing clients...");
        orderedAugLog.flatMap(new FlatMapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple5<String, String, String, Integer, Integer> object, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(object.f0, 1));
            }
        }).keyBy(0).sum(1).writeAsCsv(client_path).setParallelism(1);
        
        System.out.println("Analysing requests...");
        orderedAugLog.flatMap(new FlatMapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple5<String, String, String, Integer, Integer> object, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(object.f2, 1));
            }
        }).keyBy(0).sum(1).writeAsCsv(request_path, NO_OVERWRITE, "\n", "|||").setParallelism(1);
        
        System.out.println("Analysing resource sizes...\n");
        orderedAugLog.flatMap(new FlatMapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple5<String, String, String, Integer, Integer> object, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(object.f2, object.f4));
            }
        }).keyBy(0).writeAsCsv(resource_path, NO_OVERWRITE, "\n", "|||").setParallelism(1);
        
        // execute program
        env.execute();
        
        ExecutionEnvironment clientEnv = ExecutionEnvironment.getExecutionEnvironment();
        clientEnv.setParallelism(Integer.parseInt(cores));
        
        DataSet<Tuple2<String, Integer>> clientSet = clientEnv.readCsvFile(client_path).types(String.class, Integer.class);
        
        DataSet<Tuple2<String, Integer>> clientCounts =
                clientSet.groupBy(0).maxBy(1).sortPartition(1, Order.DESCENDING).first(1);
        ArrayList<Tuple2<String, Integer>> maxClient = (ArrayList<Tuple2<String, Integer>>) clientCounts.collect();
        String maxClientString = maxClient.get(0).f0;
        Integer maxClientCount = maxClient.get(0).f1;
        
        ExecutionEnvironment requestEnv = ExecutionEnvironment.getExecutionEnvironment();
        requestEnv.setParallelism(Integer.parseInt(cores));
        
        CsvReader requestCSV = new CsvReader(request_path, requestEnv);
        requestCSV.fieldDelimiter("|||");
        
        DataSet<Tuple2<String, Integer>> requestSet = requestCSV.types(String.class, Integer.class);
        
        DataSet<Tuple2<String, Integer>> requestCounts =
                requestSet.groupBy(0).maxBy(1).sortPartition(1, Order.DESCENDING).first(1);
        ArrayList<Tuple2<String, Integer>> maxRequest = (ArrayList<Tuple2<String, Integer>>) requestCounts.collect();
        String maxRequestString = maxRequest.get(0).f0;
        Integer maxRequestCount = maxRequest.get(0).f1;
        
        ExecutionEnvironment resourceEnv = ExecutionEnvironment.getExecutionEnvironment();
        resourceEnv.setParallelism(Integer.parseInt(cores));
        
        CsvReader resourceCSV = new CsvReader(resource_path, resourceEnv);
        resourceCSV.fieldDelimiter("|||");
        
        DataSet<Tuple2<String, Integer>> resourceSet = resourceCSV.types(String.class, Integer.class);
        
        DataSet<Tuple2<String, Integer>> resourceSize =
                resourceSet.maxBy(1).sortPartition(1, Order.DESCENDING).first(1);
        ArrayList<Tuple2<String, Integer>> largestResource = (ArrayList<Tuple2<String, Integer>>) resourceSize.collect();
        String largestResourceString = largestResource.get(0).f0;
        Integer largestResourceSize = largestResource.get(0).f1;
        
        System.out.println("Client with most requests: " + maxClientString + "(" + maxClientCount + ")");
        System.out.println("Most often requested source: " + maxRequestString + "(" + maxRequestCount + ")");
        System.out.println("Largest resource send: " + largestResourceString + "(" + largestResourceSize + " Bytes)");
        
        
        //cleanup
        if (clientFile.delete())
            System.out.println("\nclient file cleaned");
        else
            System.out.println("\nclient file error");
        if (requestFile.delete())
            System.out.println("request file cleaned");
        else
            System.out.println("request file error");
        if (resourceFile.delete())
            System.out.println("resource file cleaned");
        else
            System.out.println("resource file error");
        
    }
}
