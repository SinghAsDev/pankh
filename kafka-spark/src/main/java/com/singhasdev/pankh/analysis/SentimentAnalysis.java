package com.singhasdev.pankh.analysis;

import com.cloudera.spark.hbase.JavaHBaseContext;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class SentimentAnalysis {
  private static final Logger logger = LoggerFactory.getLogger(SentimentAnalysis.class);

  public static void main (String[] args) {
    if (args.length < 2) {
      logger.error("Usage: SentimentAnalysis <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      return;
    }

    logger.info("Starting sentiment analysis");

    String brokers = args[0];
    String topics = args[1];

    // Create context
    SparkConf sparkConf = new SparkConf().setAppName("SentimentAnalysis");
    sparkConf.setMaster("local[2]");
    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf,
        Durations.seconds(2));

    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);
    //kafkaParams.put("auto.offset.reset", "smallest");


    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        javaStreamingContext,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    // Get the json, split them into words, count the words and print
    JavaDStream<String> json = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    //json.print();

    JavaPairDStream<Long, String> tweets = json.mapToPair(new TwitterRawJsonParser());
    //tweets.print();

    JavaPairDStream<Long, String> filteredTweets = tweets.filter(new Function<Tuple2<Long, String>, Boolean>() {
      @Override
      public Boolean call(Tuple2<Long, String> tweet) throws Exception {
        return tweet != null;
      }
    });
    //filteredTweets.print();

    JavaDStream<Tuple2<Long, String>> stemmedTweets = filteredTweets.map(new StemmingFunction());

    JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets =
        stemmedTweets.mapToPair(new PositiveScoreFunction());

    JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
        stemmedTweets.mapToPair(new NegativeScoreFunction());

    JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined =
        positiveTweets.join(negativeTweets);

    JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets =
        joined.map(new Function<Tuple2<Tuple2<Long, String>,
            Tuple2<Float, Float>>,
            Tuple4<Long, String, Float, Float>>() {
          private static final long serialVersionUID = 42l;
          @Override
          public Tuple4<Long, String, Float, Float> call(
              Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet)
          {
            return new Tuple4<Long, String, Float, Float>(
                tweet._1()._1(),
                tweet._1()._2(),
                tweet._2()._1(),
                tweet._2()._2());
          }
        });

    JavaDStream<Tuple5<Long, String, Float, Float, String>> result =
        scoredTweets.map(new ScoreTweetsFunction());

    result.print();

    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(javaStreamingContext.sparkContext(), conf);

    hbaseContext.streamBulkPut(result, "tweets_1", new PutFunction(), true);

    // Start the computation
    javaStreamingContext.start();
    javaStreamingContext.awaitTermination();

    logger.info("Done with sentiment analysis");
  }

  private static class PutFunction implements Function<Tuple5<Long, String, Float, Float, String>, Put> {
    private static final byte[] family = "tweet".getBytes();
    private static final byte[] tweetText = "tweet_text".getBytes();
    private static final byte[] tweetPositiveScore = "positive_score".getBytes();
    private static final byte[] tweetNegativeScore = "negative_score".getBytes();
    private static final byte[] tweetSentiment = "sentiment".getBytes();

    @Override
    public Put call(Tuple5<Long, String, Float, Float, String> tuple5) throws Exception {
      Put put = new Put(Bytes.toBytes(tuple5._1()));
      put.add(family, tweetText, Bytes.toBytes(tuple5._2()));
      put.add(family, tweetPositiveScore, Bytes.toBytes(tuple5._3()));
      put.add(family, tweetNegativeScore, Bytes.toBytes(tuple5._4()));
      put.add(family, tweetSentiment, Bytes.toBytes(tuple5._5()));
      return put;
    }
  }
}
