# Pankh
Pankh is an application that performs sentiment analysis on tweets. The application uses 
twitter4j to get tweets from twitter, which is then streamed to Apache Kafka. Spark streaming is 
used to perform sentiment analysis on stream of tweets that it gets from Apache Kafka, 
which acts as a message bus here. Based on the analysis, each tweet is marked positive or 
negative and the result is then persisted in HBase. The results then can be used from various 
applications. For demo purposes one can easily use Hue.

This application has been tested on CDH 5.4.0 cluster.

# How to build
The applicaiton has two modules.

# twitter-kafka
*twitter-kakfa* consumes tweets from twitter using credentials provided in {{conf/producer.conf}}
 and produces those tweets to Apache Kafka whose information is also provided in {{conf/producer
 .conf}}. Modify the configuration file with suitable values before running the application.
 
## How to run
```
./gradlew :twitter-kafka:run -Pargs="<PATH_TO_CONF>"
```

If configuration file is not specified as an argument, then the app uses {{conf/producer.conf}} 
as configuration file by default.

# kafka-spark
*kafka-spark* consumes tweets from Apache Kafka whose information is provided in 
{{conf/analyzer.conf}} and performs sentiment analysis on the stream of tweets. Tweets along 
with sentiment analysis result is then persisted in Apache HBase. Modify the configuration file 
with suitable values before running the application.
 
## How to run
```
./gradlew :kafka-spark:run -Pargs="<PATH_TO_CONF>"
```

If configuration file is not specified as an argument, then the app uses {{conf/analyzer.conf}} 
as configuration file by default.

P.S.: Note that I have used bunch of existing pieces scattered on internet to put together this 
application. I have tried to adhere to any of the license requirements. If by any chance, 
you see violation of any license agreement, please contact me, @singhasdev, 
and we can get it resolved.