package com.spark.application;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class Application {

    public static String zkQuorum = "localhost:2181"; //is a list of one or more zookeeper servers that make quorum
    public static String group = "spark-consumer-group";   //<group> is the name of kafka consumer group
    public static String topic = "twitter-topic";
    public static String reciever_topic = "twitter-reciever";
    public static Integer numThreads = 2; // is the number of threads the kafka consumer should use

    public static CloseableHttpClient httpclient = HttpClients.createDefault();
    public static HttpPost httpPost = new HttpPost("http://192.168.56.102:3000/post");
    public static CloseableHttpResponse response2;
    private static final Pattern SPACE = Pattern.compile(" ");



    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                //.setMaster("local[2]");
                .setMaster("spark://192.168.56.102:7077");
        // create streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.ERROR);

        // Checkpointing must be enabled to use the updateStateByKey function.
        //jssc.checkpoint("/tmp/log-analyzer-streaming");

        // topic count map for kafka consumer
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, numThreads);
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        System.out.println("Messages created !");

        JavaDStream<String> json = messages.map(
                new Function<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = 42l;
                    @Override
                    public String call(Tuple2<String, String> message) {
                        return message._2();
                    }
                }
        );


        //Commented the Normal json => (id,tweetText)

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

       // Commented because we dont need any filters FOR NOW.
        //We may use json two times 1-> TweetToVector 2->TwitterTokenizer

        JavaDStream<Tuple2<Long, String>> tweetsFiltered = filtered.map(
                new TextFilterFunction());

/*        JavaDStream<Vector> tweetsVectorized = json.map(
                new TweetToVector());*/




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
                System.out.println();
                return null;
            }
        });*/



/*        tweetsFiltered.foreachRDD(new Function<JavaRDD<Tuple2<Long, String>>, Void>() {
            @Override
            public Void call(JavaRDD<Tuple2<Long, String>> tuple2JavaRDD) throws Exception {
                tuple2JavaRDD.foreach(new VoidFunction<Tuple2<Long, String>>() {
                    @Override
                    public void call(Tuple2<Long, String> longStringTuple2) throws Exception {
                        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                                producerConfig);
                        KeyedMessage<String, String> message = null;
                        message = new KeyedMessage<String, String>(topic, longStringTuple2._2);
                        producer.send(message);
                        producer.close();
                    }
                });
                return null;
            }
        });*/


        tweetsFiltered.foreachRDD(tuple2JavaRDD -> {
/*
            tuple2JavaRDD.foreach(longStringTuple2 -> {
                System.out.println("HELLo Motherfcuker ! " +longStringTuple2._2);
            });
*/

            tuple2JavaRDD.foreachPartition(tuple2Iterator -> {
                //KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
                while(tuple2Iterator.hasNext()){
                  //  ProducerRecord<String, String> message = null;
                   // message = new ProducerRecord<String, String>(reciever_topic, tuple2Iterator.next()._2);
                    //tweetsFiltered.print(1);
                    String tweet = tuple2Iterator.next()._2;
                    List<NameValuePair> nvps = new ArrayList<NameValuePair>();
                    nvps.add(new BasicNameValuePair("tweet", tweet ));
                    httpPost.setEntity(new UrlEncodedFormEntity(nvps));
                    response2 = httpclient.execute(httpPost);
                    System.out.println(tweet);
                    response2.close();
                   // producer.send(message);
                }

            });
        });

        //tweetsFiltered.foreachRDD(new PrinterFunction());

        //tweetsFiltered.print(1);
        
        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }


}
