package com.spark.application;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.log4j.*;


import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class Application {

    public static String zkQuorum = "localhost:2181"; //is a list of one or more zookeeper servers that make quorum
    public static String group = "spark-consumer-group";   //<group> is the name of kafka consumer group
    public static String topic = "twitter-topic";
    public static Integer numThreads = 2; // is the number of threads the kafka consumer should use

    private static final Pattern SPACE = Pattern.compile(" ");
    static StreamingKMeans model = new StreamingKMeans();


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[2]");
        // create streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(4));
        Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.ERROR);

        // Checkpointing must be enabled to use the updateStateByKey function.
        //jssc.checkpoint("/tmp/log-analyzer-streaming");

        // topic count map for kafka consumer
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, numThreads);
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> json = messages.map(
                new Function<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = 42l;
                    @Override
                    public String call(Tuple2<String, String> message) {
                        return message._2();
                    }
                }
        );


        /*
        Commented the Normal json => (id,tweetText)

        JavaPairDStream<Long, String> tweets = json.mapToPair(
                new TwitterTokenizer());

        JavaPairDStream<Long, String> filtered = tweets.filter(
                new Function<Tuple2<Long, String>, Boolean>() {
                    private static final long serialVersionUID = 42l;
                    @Override
                    public Boolean call(Tuple2<Long, String> tweet) {
                        return tweet != null;
                    }
                }
        );

        Commented because we dont need any filters FOR NOW.
        We may use json two times 1-> TweetToVector 2->TwitterTokenizer

        JavaDStream<Tuple2<Long, String>> tweetsFiltered = filtered.map(
                new TextFilterFunction());*/

        JavaDStream<Vector> tweetsVectorized = json.map(
                new TweetToVector());


        model.setDecayFactor(0.5);
        model.setK(2);
        //.setRandomCenters(numDimensions, 0.0) on http://spark.apache.org/docs/latest/mllib-clustering.html#streaming-k-means
        model.setRandomCenters(6, 100.0,2);
        //model.trainOn(tweetsVectorized);
        JavaDStream<Integer> results = model.predictOn(tweetsVectorized);


/*
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
*/

        // The processing can be manually stopped using jssc.stop();
        // just stop spark context jssc.stop(false);

        Timer timer = new Timer();
        //timer.schedule(new ModelPrinterFunction(), 0, 10000);


        //tweetsFiltered.foreachRDD(new PrinterFunction());
/*        tweetsVectorized.foreachRDD(new Function2<JavaRDD<Vector>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Vector> vectorJavaRDD, Time time) throws Exception {
                return null;
            }
        });*/
        results.foreachRDD(integerJavaRDD -> {
            integerJavaRDD.foreach(new VoidFunction<Integer>() {
                @Override
                public void call(Integer integer) throws Exception {
                    //System.out.println("Value is -> "+ integer);
                }
            });
        });
        //recentWordCounts.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }


}
