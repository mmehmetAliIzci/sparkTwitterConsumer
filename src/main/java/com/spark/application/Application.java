package com.spark.application;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class Application {

    public static String zkQuorum = "localhost:2181"; //is a list of one or more zookeeper servers that make quorum
    public static String group = "spark-consumer-group";   //<group> is the name of kafka consumer group
    public static String topic = "test";
    public static Integer numThreads = 2; // is the number of threads the kafka consumer should use

    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[2]");
        // create streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Checkpointing must be enabled to use the updateStateByKey function.
        //jssc.checkpoint("/tmp/log-analyzer-streaming");

        // topic count map for kafka consumer
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, numThreads);
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        // Get lines
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
        // Split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Arrays.asList(SPACE.split(x));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairDStream<String, Integer> recentWordCounts =
                wordCounts.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.seconds(100), Durations.seconds(10));

        // The processing can be manually stopped using jssc.stop();
        // just stop spark context jssc.stop(false);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        recentWordCounts.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }


}
