package com.singhasdev.pankh.analysis;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

public class StemmingFunction
    implements Function<Tuple3<Long, String, String>, Tuple3<Long, String, String>>
{
  @Override
  public Tuple3<Long, String, String> call(Tuple3<Long, String, String> tweet)
  {
    String text = tweet._3();
    List<String> stopWords = StopWords.getWords();
    for (String word : stopWords)
    {
      text = text.replaceAll("\\b" + word + "\\b", "");
    }
    return new Tuple3<Long, String, String>(tweet._1(), tweet._2(), text);
  }
}
