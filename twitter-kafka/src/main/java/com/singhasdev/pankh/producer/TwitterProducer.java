package com.singhasdev.pankh.producer;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterProducer {
  private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  private static final String CONSUMER_KEY_KEY = "consumerKey";
  private static final String CONSUMER_SECRET_KEY = "consumerSecret";
  private static final String ACCESS_TOKEN_KEY = "accessToken";
  private static final String ACCESS_TOKEN_SECRET_KEY = "accessTokenSecret";
  private static final String keywords = "keywords";
  private static final String printTweetsOnScreen = "printTweetsOnScreen";
  private static final String sendTweetsToKafka = "sendTweetsToKafka";

  private static final String BROKER_LIST = "metadata.broker.list";
  private static final String KAFKA_TOPIC = "kafka.topic";
  private static final String SERIALIZER = "serializer.class";
  private static final String REQUIRED_ACKS = "request.required.acks";

  /** Information necessary for accessing the Twitter API */
  private String consumerKey;
  private String consumerSecret;
  private String accessToken;
  private String accessTokenSecret;

  /** The actual Twitter stream. It's set up to collect raw JSON data */
  private TwitterStream twitterStream;

  private void start(final Context context) {

    /** Producer properties **/
    Properties props = new Properties();
    props.put("metadata.broker.list", context.getString(BROKER_LIST));
    props.put("serializer.class", context.getString(SERIALIZER));
    props.put("request.required.acks", context.getString(REQUIRED_ACKS));

    ProducerConfig config = new ProducerConfig(props);
    final Producer<String, String> producer = new Producer<String, String>(config);

    /** Twitter properties **/
    consumerKey = context.getString(CONSUMER_KEY_KEY);
    consumerSecret = context.getString(CONSUMER_SECRET_KEY);
    accessToken = context.getString(ACCESS_TOKEN_KEY);
    accessTokenSecret = context.getString(ACCESS_TOKEN_SECRET_KEY);

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setOAuthConsumerKey(consumerKey);
    cb.setOAuthConsumerSecret(consumerSecret);
    cb.setOAuthAccessToken(accessToken);
    cb.setOAuthAccessTokenSecret(accessTokenSecret);
    cb.setJSONStoreEnabled(true);
    cb.setIncludeEntitiesEnabled(true);

    twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    final boolean shouldPrintTweetsOnScreen = Boolean.parseBoolean(context.getString
        (printTweetsOnScreen));
    final boolean shouldSendTweetsToKafka = Boolean.parseBoolean(context.getString
        (sendTweetsToKafka));

    final StatusListener listener = new StatusListener() {
      // The onStatus method is executed every time a new tweet comes
      // in.
      public void onStatus(Status status) {
        // The EventBuilder is used to build an event using the
        // the raw JSON of a tweet
        if (shouldPrintTweetsOnScreen) {
          logger.info(status.getUser().getScreenName() + ": " + status.getText());
        }

        if (shouldSendTweetsToKafka) {
          KeyedMessage<String, String> data = new KeyedMessage<String, String>(context.getString(KAFKA_TOPIC)
              , TwitterObjectFactory.getRawJSON(status));
          producer.send(data);
        }
      }

      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

      public void onScrubGeo(long userId, long upToStatusId) {}

      public void onException(Exception ex) {
        logger.info("Shutting down Twitter sample stream...");
        twitterStream.shutdown();
      }

      public void onStallWarning(StallWarning warning) {}
    };

    twitterStream.addListener(listener);

    twitterStream.filter(new FilterQuery().track(context.getString(keywords).split(",")));
  }

  public static void main(String[] args) {
    String confFile;
    if (args.length != 1) {
      logger.warn("A config file is expected as argument. Using default file, conf/producer.conf");
      confFile = "conf/producer.conf";
    } else {
      confFile = args[0];
    }

    try {
      Context context = new Context(confFile);
      TwitterProducer tp = new TwitterProducer();
      tp.start(context);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}